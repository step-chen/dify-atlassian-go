package main

import (
	"context"
	"flag" // Add flag package
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	confluence_cfg "github.com/step-chen/dify-atlassian-go/internal/config/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	difyClients map[string]*dify.Client
	batchPool   *batchpool.BatchPool
	cfg         *confluence_cfg.Config // Use the specific Confluence config type
)

// Application entry point, handles initialization and task processing
func main() {
	// Define command-line flag for config file path
	configFile := flag.String("c", "config.yaml", "Path to the configuration file")
	flag.Parse() // Parse command-line flags

	// Load config file using the confluence config loader
	var err error
	cfg, err = confluence_cfg.LoadConfig(*configFile) // Use the flag value
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	// Initialize required tools
	utils.InitRequiredTools()

	// Init batch pool
	// Use BatchPoolSize for max workers, QueueSize for the internal task queue
	// Pass the Concurrency part of the loaded config to the constructor
	batchPool = batchpool.NewBatchPool(
		cfg.Concurrency.BatchPoolSize,
		cfg.Concurrency.QueueSize,
		func(ctx context.Context, key, id, title, batch string, op batchpool.Operation) (string, error) {
			return difyClients[key].CheckBatchStatus(
				ctx,
				key,
				id,
				title,
				batch,
				op,
				cfg.Concurrency.IndexingTimeout,
				cfg.Concurrency.DeleteTimeoutContent,
			)
		},
		cfg.Concurrency,
	)

	// Run the main processing loop
	runProcessingLoop()

	// Close the batch pool gracefully
	batchPool.Close()

	log.Println("Processing complete.")
}

// runProcessingLoop initializes clients, workers, and manages the space processing loop with retries.
func runProcessingLoop() {
	// Init Dify clients per space
	difyClients = make(map[string]*dify.Client)
	for _, spaceKey := range cfg.Confluence.SpaceKeys { // Use ConfluenceSettings
		datasetID, exists := cfg.Dify.Datasets[spaceKey]
		if !exists {
			log.Fatalf("no dataset mapping configured for space key: %s", spaceKey)
		}
		if datasetID == "" {
			log.Fatalf("dataset_id is missing for space key: %s", spaceKey)
		}
		client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, datasetID, cfg)
		if err != nil {
			log.Fatalf("failed to create Dify client for space %s (dataset %s): %v", spaceKey, datasetID, err)
		}
		if err = client.InitMetadata(); err != nil {
			log.Fatalf("failed to initialize metadata for space %s: %v", spaceKey, err)
		}
		difyClients[spaceKey] = client
	}

	// Init Confluence client
	confluenceClient, err := confluence.NewClient(cfg.Confluence.BaseURL, cfg.Confluence.APIKey, cfg.AllowedTypes, cfg.UnsupportedTypes) // Use ConfluenceSettings
	if err != nil {
		log.Fatalf("failed to create Confluence client: %v", err)
	}

	// Create worker pool with queue size
	jobChannels := JobChannels{
		Jobs: make(chan Job, cfg.Concurrency.QueueSize),
	}
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < cfg.Concurrency.Workers; i++ {
		wg.Add(1)
		go worker(jobChannels.Jobs, &wg)
	}

	// Process spaces with retries
	for i := 0; i < cfg.Concurrency.MaxRetries+1; i++ {
		cfg.Concurrency.IndexingTimeout = (i + 1) * cfg.Concurrency.IndexingTimeout // Increase timeout for each retry

		// Process all spaces
		for _, spaceKey := range cfg.Confluence.SpaceKeys { // Use ConfluenceSettings
			c, exists := difyClients[spaceKey]
			if !exists {
				log.Printf("Warning: Dify client not found for space %s during processing run %d. Skipping.", spaceKey, i+1)
				continue
			}
			if err := processSpace(spaceKey, c, confluenceClient, &jobChannels); err != nil {
				log.Printf("error processing space %s during run %d: %v", spaceKey, i+1, err)
			}
		}

		// Wait for batch monitoring tasks from this run to complete
		log.Printf("Waiting for batch monitoring tasks to complete for run %d...", i+1)
		batchPool.Wait()
		log.Printf("Batch monitoring complete for run %d.", i+1)

		// Check if there are any timed-out items to retry
		if !batchPool.HasTimeoutItems() {
			log.Printf("No timed-out items found after run %d. Processing finished.", i+1)
			break // Exit retry loop if no timeouts occurred
		}

		log.Printf("Preparing for retry after run %d.", i+1)
		// Reset timeoutContents for the next potential retry run (or if this was the last run)
		// The actual retry logic happens within processSpace based on the timeoutContents populated by statusChecker
		batchPool.ClearTimeoutContents()

		if i == cfg.Concurrency.MaxRetries {
			log.Printf("Max retries (%d) reached. Some items may not have been processed successfully.", cfg.Concurrency.MaxRetries)
		}
	}

	// Close the job channel *after* submitting initial jobs AND all retry jobs
	log.Println("Closing job channel...")
	close(jobChannels.Jobs)

	// Wait for all worker goroutines to finish processing ALL jobs (initial + retries)
	log.Println("Waiting for all workers to complete...")
	wg.Wait()
	log.Println("All workers finished.")

	// Log failed types (remains unchanged)
	utils.WriteFailedTypesLog()
}
