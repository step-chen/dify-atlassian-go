package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/concurrency"
	"github.com/step-chen/dify-atlassian-go/internal/config"
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	difyClients     map[string]*dify.Client
	batchPool       *concurrency.BatchPool
	timeoutContents map[string]map[string]confluence.ContentOperation // Stores map[spaceKey]map[contentID]confluence.ContentOperation
	timeoutMutex    sync.Mutex                                        // Mutex for protecting timeoutContents
	cfg             *config.Config                                    // Global configuration
)

// Application entry point, handles initialization and task processing
func main() {
	// Load config file
	var err error
	cfg, err = config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize required tools
	utils.InitRequiredTools()

	// Init timeout contents map
	timeoutContents = make(map[string]map[string]confluence.ContentOperation)

	// Init batch pool
	// Use BatchPoolSize for max workers, QueueSize for the internal task queue
	// Pass the loaded config (cfg) to the constructor
	batchPool = concurrency.NewBatchPool(cfg.Concurrency.BatchPoolSize, cfg.Concurrency.QueueSize, statusChecker, cfg)

	// Init Dify clients per space
	difyClients = make(map[string]*dify.Client)
	for _, spaceKey := range cfg.Confluence.SpaceKeys {
		datasetID, exists := cfg.Dify.Datasets[spaceKey]
		if !exists {
			log.Fatalf("no dataset_id configured for space key: %s", spaceKey)
		}

		client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, datasetID, cfg)
		if err != nil {
			log.Fatalf("failed to create Dify client for space %s: %v", spaceKey, err)
		}
		if err = client.InitMetadata(); err != nil {
			log.Fatalf("failed to initialize metadata for space %s: %v", spaceKey, err)
		}
		difyClients[spaceKey] = client
	}

	// Init Confluence client
	confluenceClient, err := confluence.NewClient(cfg.Confluence.BaseURL, cfg.Confluence.APIKey, cfg.AllowedTypes, cfg.UnsupportedTypes)
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

	for i := 0; i < cfg.Concurrency.MaxRetries+1; i++ {
		cfg.Concurrency.IndexingTimeout = (i + 1) * cfg.Concurrency.IndexingTimeout // Increase timeout for each retry
		// Process all spaces
		for _, spaceKey := range cfg.Confluence.SpaceKeys {
			c := difyClients[spaceKey]
			if err := processSpace(spaceKey, c, confluenceClient, &jobChannels); err != nil {
				log.Printf("error processing space %s: %v", spaceKey, err)
			}
		}
		// Now, wait for all submitted batch monitoring tasks from the initial run to complete
		// The job channel will be closed later, after potential retry jobs are submitted.
		log.Println("Waiting for initial batch monitoring tasks to complete...")
		batchPool.Wait()
		log.Println("Initial batch monitoring complete.")

		timeoutContents = make(map[string]map[string]confluence.ContentOperation) // Clear timeout contents for next retry
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

	// Close the batch pool gracefully
	batchPool.Close()

	log.Println("Processing complete.")
}

// Check batch status using Dify client
// This function is passed to the BatchPool and runs within its workers.
// It now receives a context from the BatchPool.
func statusChecker(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error) {
	// Check for context cancellation first (from BatchPool's task timeout or global shutdown)
	select {
	case <-ctx.Done():
		return "", ctx.Err() // Propagate context error
	default:
		// Proceed with status check if context is not done
	}

	client, exists := difyClients[spaceKey]
	if !exists {
		return "", fmt.Errorf("no Dify client for space %s", spaceKey)
	}

	status, err := client.GetIndexingStatus(spaceKey, batch)
	if err != nil {
		if err.Error() == "unexpected status code: 404" {
			return "completed", nil
		} else {
			return "", err
		}
	}
	if len(status.Data) > 0 {
		if status.Data[0].IndexingStatus == "completed" {
			return "completed", nil
		}
		op.StartAt = status.LastStepAt()

		// Check if ProcessingStartedAt is more than 2 minutes ago
		if time.Since(op.StartAt) > time.Duration(cfg.Concurrency.IndexingTimeout)*time.Minute {
			fmt.Println("timeout:", op.StartAt, time.Since(op.StartAt), time.Now())

			if cfg.Concurrency.DeleteTimeoutContent {
				// Delete the document or update metadata if configured to do so
				// Pass the confluenceID which is a parameter of statusChecker
				err := client.DeleteDocument(status.Data[0].ID, confluenceID)
				if err != nil {
					// Error message updated to reflect potential metadata update failure too
					return "", fmt.Errorf("failed to delete/update timeout document %s for %s content %s: %w", status.Data[0].ID, spaceKey, title, err)
				}

				// Store and log the ID for retry
				if op.Action == 0 {
					op.Action = 1 // Mark as needing retry (deletion happened)
				}
				timeoutMutex.Lock()
				if timeoutContents[spaceKey] == nil {
					timeoutContents[spaceKey] = make(map[string]confluence.ContentOperation)
				}
				if status.Data[0].ID != "" {
					op.DifyID = status.Data[0].ID
				}
				timeoutContents[spaceKey][confluenceID] = op
				timeoutMutex.Unlock()

				return "deleted", nil // Marked as deleted, will be retried later
			} else {
				// If not configured to delete, simply mark as completed and don't retry
				log.Printf("Indexing timed out for %s (%s), but configured not to delete. Marking as completed.", title, confluenceID)
				return "completed", nil
			}
		}
		return status.Data[0].IndexingStatus, nil
	}

	return "", fmt.Errorf("no status data found")
}
