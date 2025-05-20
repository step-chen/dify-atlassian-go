package main

import (
	"context"
	"flag"
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	directory_cfg "github.com/step-chen/dify-atlassian-go/internal/config/directory"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	difyClients map[string]*dify.Client
	batchPool   *batchpool.BatchPool
	cfg         *directory_cfg.Config
)

func main() {
	// Define command-line flag for config file path
	configFile := flag.String("c", "config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load config file using the directory config loader
	var err error
	cfg, err = directory_cfg.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	// Initialize required tools
	utils.InitRequiredTools(utils.ToolMarkitdown | utils.ToolPandoc)

	// Initialize batch pool
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

func runProcessingLoop() {
	// Init Dify clients per directory
	difyClients = make(map[string]*dify.Client)
	for dirKey := range cfg.Directory.Path {
		datasetID, exists := cfg.Dify.Datasets[dirKey]
		if !exists {
			log.Fatalf("no dataset mapping configured for directory key: %s", dirKey)
		}
		if datasetID == "" {
			log.Fatalf("dataset_id is missing for directory key: %s", dirKey)
		}
		client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, datasetID, cfg)
		if err != nil {
			log.Fatalf("failed to create Dify client for directory %s (dataset %s): %v", dirKey, datasetID, err)
		}
		if err = client.InitMetadata(); err != nil {
			log.Fatalf("failed to initialize metadata for directory %s: %v", dirKey, err)
		}
		difyClients[dirKey] = client
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

	// Process directories with retries
	for i := 0; i < cfg.Concurrency.MaxRetries+1; i++ {
		cfg.Concurrency.IndexingTimeout = (i + 1) * cfg.Concurrency.IndexingTimeout

		// Process all directories
		for dirKey, dirPath := range cfg.Directory.Path {
			c, exists := difyClients[dirKey]
			if !exists {
				log.Printf("Warning: Dify client not found for directory %s during processing run %d. Skipping.", dirKey, i+1)
				continue
			}
			if err := processDirectory(dirKey, dirPath, c, &jobChannels); err != nil {
				log.Printf("error processing directory %s during run %d: %v", dirKey, i+1, err)
			}
		}

		// Wait for batch monitoring tasks to complete
		log.Printf("Waiting for batch monitoring tasks to complete for run %d...", i+1)
		batchPool.Wait()
		log.Printf("Batch monitoring complete for run %d.", i+1)

		// Check if there are any timed-out items to retry
		if !batchPool.HasTimeoutItems() {
			log.Printf("No timed-out items found after run %d. Processing finished.", i+1)
			break
		}

		log.Printf("Preparing for retry after run %d.", i+1)
		batchPool.ClearTimeoutContents()

		if i == cfg.Concurrency.MaxRetries {
			log.Printf("Max retries (%d) reached. Some items may not have been processed successfully.", cfg.Concurrency.MaxRetries)
		}
	}

	// Close the job channel
	log.Println("Closing job channel...")
	close(jobChannels.Jobs)

	// Wait for all workers to complete
	log.Println("Waiting for all workers to complete...")
	wg.Wait()
	log.Println("All workers finished.")

	// Log failed types
	utils.WriteFailedTypesLog()
}
