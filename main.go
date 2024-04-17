package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-resty/resty/v2"
)

type Item struct {
	AdvertisingInventoryID       int    `json:"advertising_inventory_id"`
	CrossRecommendationAmount    int    `json:"cross_recommendation_amount"`
	MpcObjectID                  string `json:"mpc_object_id"`
	CmsObjectID                  string `json:"cms_object_id"`
	SiteID                       string `json:"site_id"`
	AdvertisingInventoryCapacity int    `json:"advertising_inventory_capacity"`
	GeographicArea               string `json:"geographic_area"`
	CustomerID                   string `json:"customer_id"`
	RecommendationCD             string `json:"recommendation_cd"`
}

type APIResponse struct {
	CustomerID              string `json:"customer_id"`
	RecommendationRequestID int64  `json:"recommendation_request_id"`
	Items                   []struct {
		ClickURL          string `json:"click_url"`
		CmsObjectID       string `json:"cms_object_id"`
		MpcObjectID       string `json:"mpc_object_id"`
		ObjectPositionNum int    `json:"object_position_num"`
	} `json:"items"`
}

var cfg *Config

func main() {

	cfg = NewConfig()

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisHost,
		DB:   cfg.RedisDb,
	})

	restClient := resty.New()

	resultCh := make(chan APIResponse)
	failedCh := make(chan []Item)

	sem := make(chan struct{}, cfg.MaxConcurrentReqs)
	var wg sync.WaitGroup

	wroteDone := make(chan bool)

	go writeResultsToRedis(rdb, resultCh, wroteDone)

	items, err := fetchItemsFromRedis(rdb)

	if err != nil {
		log.Fatalf("Error fetching items from Redis: %v", err)
	}
	log.Printf("Fetched %d items from Redis", len(items))

	batches := splitIntoBatches(items, cfg.BatchSize)

	// Main loop
	for tries := 0; tries < cfg.MaxRetries; tries++ {
		if len(batches) == 0 {
			log.Printf("No items left to process. Exiting...")
			break
		}

		for _, batch := range batches {
			sem <- struct{}{} // Acquire semaphore
			wg.Add(1)
			go func(batch []Item) {
				defer func() {
					<-sem // Release semaphore
					wg.Done()
				}()

				if processedBatch, err := postBatchToAPI(restClient, batch); err != nil {
					log.Printf("Error posting batch to API: %v", err)
					failedCh <- batch
				} else {
					for _, res := range processedBatch {
						resultCh <- res
					}

				}

			}(batch)
		}

		wg.Wait()

		close(failedCh)

		batches = batches[:0]
		for failedBatch := range failedCh {
			batches = append(batches, failedBatch)
		}

		if len(batches) == 0 {
			log.Printf("All items processed successfully")
			break
		}
	}
	close(resultCh)
	<-wroteDone
}

func fetchItemsFromRedis(rdb *redis.Client) ([]Item, error) {
	ctx := context.Background()
	result, err := rdb.LRange(ctx, cfg.RedisList, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var items []Item
	for _, itemID := range result {
		clientID, err := strconv.Atoi(itemID)
		if err != nil {
			log.Printf("Error converting client ID to int: %v", err)
			continue
		}
		item := Item{
			AdvertisingInventoryID:       0,
			CrossRecommendationAmount:    0,
			MpcObjectID:                  "0",
			CmsObjectID:                  "0",
			SiteID:                       "4",
			AdvertisingInventoryCapacity: 20,
			GeographicArea:               "msk",
			CustomerID:                   strconv.Itoa(clientID),
			RecommendationCD:             "I2P",
		}
		items = append(items, item)
	}

	return items, nil
}

func splitIntoBatches(items []Item, batchSize int) [][]Item {
	var batches [][]Item

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}

	return batches
}

func postBatchToAPI(restClient *resty.Client, batch []Item) ([]APIResponse, error) {

	startTime := time.Now()
	resp, err := restClient.R().
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+cfg.AuthToken).
		SetHeader("apikey", cfg.ApiKey).
		SetBody(batch).
		Post(cfg.ApiEndpoint)

	log.Printf("Time taken to fetch the batch: %s", time.Since(startTime))

	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("non-200 status code received: %d", resp.StatusCode())
	}

	var processedBatch []APIResponse
	if err := json.Unmarshal(resp.Body(), &processedBatch); err != nil {
		log.Printf("Error unmarshaling API response: %v", err)
		log.Printf("API response body: %s", resp.Body())
		return nil, fmt.Errorf("error unmarshaling API response")
	}

	return processedBatch, nil
}

func writeResultsToRedis(rdb *redis.Client, resultCh <-chan APIResponse, done chan bool) {
	ctx := context.Background()
	var recordsWritten int
	pipeline := rdb.TxPipeline()

	for res := range resultCh {
		recordsWritten++
		var items []string
		for _, item := range res.Items {
			items = append(items, item.CmsObjectID)
		}
		itemsStr := strings.Join(items, ", ")
		pipeline.HSet(ctx, cfg.RedisResult, res.CustomerID, itemsStr)
		if recordsWritten >= 10000 {
			panicIfError(pipeline.Exec(ctx))
			pipeline = rdb.TxPipeline()
			log.Printf("Wrote %d records to Redis", recordsWritten)
			recordsWritten = 0
		}
	}
	if recordsWritten > 0 {
		panicIfError(pipeline.Exec(ctx))
	}
	log.Printf("Wrote %d records to Redis", recordsWritten)
	done <- true
}

func panicIfError(_ interface{}, err error) {
	if err != nil {
		panic(err)
	}
}
