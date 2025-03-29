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

	// Process all spaces
	for _, spaceKey := range cfg.Confluence.SpaceKeys {
		c := difyClients[spaceKey]
		docMetas, err := c.FetchDocumentsList(0, 100)
		if err != nil {
			log.Printf("failed to list documents for space %s (dataset: %s): %v", spaceKey, c.DatasetID(), err)
		}
		if err := processSpace(spaceKey, c, confluenceClient, &jobChannels, docMetas); err != nil {
			log.Printf("error processing space %s: %v", spaceKey, err)
		}
	}

	// Now, wait for all submitted batch monitoring tasks from the initial run to complete
	// The job channel will be closed later, after potential retry jobs are submitted.
	log.Println("Waiting for initial batch monitoring tasks to complete...")
	batchPool.Wait()
	log.Println("Initial batch monitoring complete.")

	// Retry processing timed out contents up to MaxRetries times
	for i := 0; i < cfg.Concurrency.MaxRetries; i++ {
		cfg.Concurrency.IndexingTimeout = (i + 2) * cfg.Concurrency.IndexingTimeout // Increase timeout for each retry
		log.Printf("Retry attempt %d/%d for timed out documents...", i+1, cfg.Concurrency.MaxRetries)
		processTimedOutContents(difyClients, confluenceClient, &jobChannels)

		// Wait for batch monitoring tasks submitted during this retry attempt to complete
		log.Printf("Waiting for batch monitoring tasks from retry attempt %d to complete...", i+1)
		batchPool.Wait()
		log.Printf("Batch monitoring tasks from retry attempt %d complete.", i+1)

		// Check if there are still timed out contents after processing and waiting
		timeoutMutex.Lock()
		remainingTimeouts := len(timeoutContents)
		timeoutMutex.Unlock()

		if remainingTimeouts == 0 {
			log.Println("No more timed out documents to retry.")
			break // Exit loop if no more timeouts
		}
		log.Printf("Found %d remaining timed out documents after attempt %d. Continuing retry if attempts remain.", remainingTimeouts, i+1)
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

	// Log 404 errors recorded by the batch pool (remains unchanged)
	batchPool.LogNotFoundErrors()

	// Close the batch pool gracefully
	batchPool.Close()

	log.Println("Processing complete.")
}

// processTimedOutContents handles the logic for retrying documents that timed out during indexing.
func processTimedOutContents(difyClients map[string]*dify.Client, confluenceClient *confluence.Client, jobChannels *JobChannels) {
	log.Println("Checking for timed out documents to retry...")
	var copiedTimeoutContents map[string]map[string]confluence.ContentOperation

	timeoutMutex.Lock() // Lock before checking and copying timeoutContents
	if len(timeoutContents) > 0 {
		log.Printf("Found %d spaces with timed out documents. Preparing for retry.", len(timeoutContents))
		copiedTimeoutContents = prepareTimeoutContents() // Copies and clears the global map
	} else {
		log.Println("No timed out documents found needing retry.")
	}
	timeoutMutex.Unlock() // Unlock after check and copy/clear

	// Process the copied timed-out items outside the lock
	if len(copiedTimeoutContents) > 0 {
		log.Println("Submitting retry jobs for timed out documents...")
		for spaceKey, spaceContents := range copiedTimeoutContents {
			client, exists := difyClients[spaceKey]
			if !exists {
				log.Printf("Error processing retry: No Dify client found for space %s. Skipping.", spaceKey)
				continue
			}
			for contentID, operation := range spaceContents {
				log.Printf("Retrying content %s in space %s", contentID, spaceKey)
				// Ensure jobChannels is passed correctly as a pointer
				if err := processContentOperation(contentID, operation, spaceKey, client, confluenceClient, jobChannels); err != nil {
					// Log error but continue processing other retries
					log.Printf("Error submitting retry job for content %s in space %s: %v", contentID, spaceKey, err)
				}
			}
		}
		log.Println("Finished submitting retry jobs for timed out documents.")
	}
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
		return "", err
	}
	if len(status.Data) > 0 {
		if status.Data[0].IndexingStatus == "completed" {
			return "completed", nil
		}
		// Check if ProcessingStartedAt is more than 2 minutes ago
		if time.Since(op.StartAt) > time.Duration(cfg.Concurrency.IndexingTimeout)*time.Minute {
			fmt.Println("timeout:", op.StartAt, time.Since(op.StartAt), time.Now())
			// Delete the document
			err := client.DeleteDocument(status.Data[0].ID)
			if err != nil {
				return "", fmt.Errorf("failed to delete timeout document for %s content %s: %w", spaceKey, title, err)
			}

			// Store and log the ID
			if op.Action == 0 {
				op.Action = 1
			}
			// Store the ID for retry using mutex
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
		}
		return status.Data[0].IndexingStatus, nil
	}

	return "", fmt.Errorf("no status data found")
}
