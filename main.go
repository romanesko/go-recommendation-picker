package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-resty/resty/v2"
)

const (
	batchSize         = 250
	redisList         = "PICKER:REGION_CLIENTS_LIST:MSK"
	redisResult       = "PICKER:TEST"
	apiEndpoint       = "https://api.mts.ru/ESAUL_REST_API/0.1.2/esaul/dcc/recommendations"
	maxConcurrentReqs = 10
	maxRetries        = 3
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

func main() {
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "mailings-01.ticketland.ru:6379", // Redis server address
		Password: "",                               // No password set
		DB:       1,                                // Use the default DB
	})

	// Initialize REST client
	restClient := resty.New()

	// Channel to receive processed batches
	resultCh := make(chan APIResponse)
	// Channel to handle failed requests
	failedCh := make(chan []Item)

	// Semaphore to limit concurrent API requests
	sem := make(chan struct{}, maxConcurrentReqs)
	var wg sync.WaitGroup

	go writeResultsToRedis(rdb, resultCh)

	items, err := fetchItemsFromRedis(rdb)

	if err != nil {
		log.Fatalf("Error fetching items from Redis: %v", err)
	}
	log.Printf("Fetched %d items from Redis", len(items))

	batches := splitIntoBatches(items, batchSize)

	// Main loop
	for tries := 0; tries < maxRetries; tries++ {
		if len(batches) == 0 {
			log.Println("No items left to process. Exiting...")
			break
		}

		for _, batch := range batches {
			sem <- struct{}{} // Acquire semaphore token
			wg.Add(1)
			go func(batch []Item) {
				defer func() {
					<-sem // Release semaphore token
					wg.Done()
				}()

				if processedBatch, err := postBatchToAPI(restClient, batch); err != nil {
					log.Printf("Error posting batch to API: %v", err)
					failedCh <- batch // Put the failed chunk into the failed queue
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
			log.Println("All items processed successfully")
			break
		} else {

		}
	}
}

func fetchItemsFromRedis(rdb *redis.Client) ([]Item, error) {
	ctx := context.Background()
	result, err := rdb.LRange(ctx, redisList, 0, -1).Result()
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
			CustomerID:                   strconv.Itoa(clientID), // Convert client ID to string
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
	mtsAuthToken := os.Getenv("MTS_AUTH_TOKEN") // Get MTS Auth Token from environment variable
	mtsApiKey := os.Getenv("MTS_API_KEY")       // Get MTS API Key from environment variable

	startTime := time.Now()
	resp, err := restClient.R().
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+mtsAuthToken).
		SetHeader("apikey", mtsApiKey).
		SetBody(batch).
		Post(apiEndpoint)

	fmt.Println("Time taken to fetch the batch: ", time.Since(startTime))

	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("non-200 status code received: %d", resp.StatusCode())
	}

	// Try to unmarshal API response into slice of APIResponse structs
	var processedBatch []APIResponse
	if err := json.Unmarshal(resp.Body(), &processedBatch); err != nil {
		// If unmarshaling fails, log the response body as a string
		log.Printf("Error unmarshaling API response: %v", err)
		log.Printf("API response body: %s", resp.Body())
		// Return an error
		return nil, fmt.Errorf("error unmarshaling API response")
	}

	return processedBatch, nil
}

func writeResultsToRedis(rdb *redis.Client, resultCh <-chan APIResponse) {
	ctx := context.Background()
	var recordsWritten int
	for res := range resultCh {
		// Increment recordsWritten count
		recordsWritten++

		// Transform response into desired format
		var items []string
		for _, item := range res.Items {
			items = append(items, item.CmsObjectID)
		}
		itemsStr := strings.Join(items, ", ")

		// Store transformed result into Redis hash
		_, err := rdb.HSet(ctx, redisResult, res.CustomerID, itemsStr).Result()
		if err != nil {
			log.Printf("Error storing result into Redis: %v", err)
			continue
		}
	}
	log.Printf("Wrote %d records to Redis", recordsWritten)
}
