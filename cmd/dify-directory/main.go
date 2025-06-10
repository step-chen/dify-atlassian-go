package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	CFG "github.com/step-chen/dify-atlassian-go/internal/config/directory"
	"github.com/step-chen/dify-atlassian-go/internal/dify"

	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	difyClients map[string]*dify.Client
	batchPool   *batchpool.BatchPool
	cfg         *CFG.Config
)

func main() {
	// Define command-line flag for config file path
	configFile := flag.String("c", "config.yaml", "Path to the configuration file")
	flag.Parse()

	// Load config file using the directory config loader
	var err error
	cfg, err = CFG.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	// Initialize required tools
	utils.InitRequiredTools(utils.ToolMarkitdown | utils.ToolPandoc)

	// Initialize batch pool
	batchPool = batchpool.NewBatchPool(
		cfg.Concurrency.BatchPoolSize,
		cfg.Concurrency.QueueSize,
		func(ctx context.Context, key, id, title, batch string, op batchpool.Operation) (int, string, batchpool.Operation, error) {
			if c, ok := difyClients[key]; !ok {
				err := fmt.Errorf("dify client not found for key %s in statusChecker", key)
				log.Println(err.Error())
				return -1, "", op, err
			} else {
				return c.CheckBatchStatus(ctx, key, id, title, batch, "file", op, cfg.Concurrency.IndexingTimeout, cfg.Concurrency.DeleteTimeoutContent)
			}
		},
		nil,
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
	for _, cfgDir := range cfg.Directory.Paths {
		datasetID, exists := cfg.Dify.Datasets[cfgDir.Name]
		if !exists {
			log.Fatalf("no dataset mapping configured for directory key: %s", cfgDir.Name)
		}
		if datasetID == "" {
			log.Fatalf("dataset_id is missing for directory key: %s", cfgDir.Name)
		}
		client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, datasetID, cfg, false)
		if err != nil {
			log.Fatalf("failed to create Dify client for directory key %s (dataset %s): %v", cfgDir.Name, datasetID, err)
		}
		if err = client.InitMetadata(); err != nil {
			log.Fatalf("failed to initialize metadata for directory key %s: %v", cfgDir.Name, err)
		}
		difyClients[cfgDir.Name] = client
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
		for _, cfgDir := range cfg.Directory.Paths {
			c, exists := difyClients[cfgDir.Name]
			if !exists {
				log.Printf("Warning: Dify client not found for directory key %s during processing run %d. Skipping.", cfgDir.Name, i+1)
				continue
			}
			if err := processDirectory(cfgDir, c, &jobChannels); err != nil {
				log.Printf("error processing directory key %s during run %d: %v", cfgDir.Name, i+1, err)
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
