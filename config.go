package main

import (
	"os"
	"strconv"
)

type Config struct {
	BatchSize         int
	RedisList         string
	RedisResult       string
	ApiEndpoint       string
	MaxConcurrentReqs int
	MaxRetries        int
	RedisHost         string
	RedisDb           int
	AuthToken         string
	ApiKey            string
}

func NewConfig() *Config {
	return &Config{
		BatchSize:         getEnvAsInt("BATCH_SIZE"),
		RedisList:         getEnv("REDIS_LIST"),
		RedisResult:       getEnv("REDIS_RESULT"),
		ApiEndpoint:       getEnv("API_ENDPOINT"),
		MaxConcurrentReqs: getEnvAsInt("MAX_CONCURRENT_REQS"),
		MaxRetries:        getEnvAsInt("MAX_RETRIES"),
		RedisHost:         getEnv("REDIS_HOST"),
		RedisDb:           getEnvAsInt("REDIS_DB"),
		AuthToken:         getEnv("AUTH_TOKEN"),
		ApiKey:            getEnv("API_KEY"),
	}
}

func getEnv(key string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	panic("Environment variable " + key + " is not set")

}

func getEnvAsInt(key string) int {
	valueStr := getEnv(key)
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	panic("Environment variable " + key + " is not set")
}
