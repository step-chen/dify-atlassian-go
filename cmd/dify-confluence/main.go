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
	timeoutContents map[string]map[string]confluence.ContentOperation // Stores IDs of timeout documents and their space keys
	cfg             *config.Config                                    // Global configuration
)

// main is the entry point of the application
// It performs the following steps:
// 1. Loads configuration
// 2. Checks and sets up environment
// 3. Initializes database
// 4. Initializes Dify clients for each space
// 5. Initializes Confluence client
// 6. Cleans up invalid mapping records
// 7. Creates worker pool
// 8. Processes all space keys
// 9. Waits for workers to complete
func main() {
	// Load configuration file from current directory
	var err error
	cfg, err = config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Check and setup environment
	/*if err := utils.CheckEnvironment(); err != nil {
		log.Fatalf("Environment setup failed: %v", err)
	}*/

	// Check required docker images
	utils.InitRequiredTools()

	// Initialize timeoutContents map
	timeoutContents = make(map[string]map[string]confluence.ContentOperation)

	// Initialize BatchPool
	batchPool = concurrency.NewBatchPool(cfg.Concurrency.BatchPoolSize, statusChecker)

	// Initialize Dify clients for each configured space
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

	// Initialize Confluence client
	confluenceClient, err := confluence.NewClient(cfg.Confluence.BaseURL, cfg.Confluence.APIKey, cfg.AllowedTypes, cfg.UnsupportedTypes)
	if err != nil {
		log.Fatalf("failed to create Confluence client: %v", err)
	}

	// Create worker pool with configured queue size
	jobChannels := JobChannels{
		Jobs: make(chan Job, cfg.Concurrency.QueueSize),
	}
	var wg sync.WaitGroup

	// Start configured number of workers
	for i := 0; i < cfg.Concurrency.Workers; i++ {
		wg.Add(1)
		go worker(jobChannels.Jobs, &wg)
	}

	// Process all configured space keys
	for _, spaceKey := range cfg.Confluence.SpaceKeys {
		c := difyClients[spaceKey]
		docMetas, err := c.FetchDocumentsList(0, 100)
		if err != nil {
			log.Printf("failed to list documents for space %s (dataset: %s): %v", spaceKey, c.DatasetID(), err)
		}
		if err := processSpace(spaceKey, c, confluenceClient, &jobChannels, docMetas); err != nil {
			log.Printf("error processing space %s: %v", spaceKey, err)
		}
		/*if err := processSpaceOperations(spaceKey, c, confluenceClient, &jobChannels); err != nil {
			log.Printf("Error processing space %s: %v", spaceKey, err)
		}*/
	}

	wg.Wait()
	retries := 0
	for retries < cfg.Concurrency.MaxRetries && len(timeoutContents) > 0 {
		cfg.Concurrency.IndexingTimeout += cfg.Concurrency.IndexingTimeout
		if err := processTimeoutContents(confluenceClient, &jobChannels); err != nil {
			log.Printf("error processing timeout documents (attempt %d/%d): %v",
				retries+1, cfg.Concurrency.MaxRetries, err)
		}
		retries++
		wg.Wait()
	}
	if len(timeoutContents) > 0 {
		log.Printf("failed to process all timeout documents after %d attempts",
			cfg.Concurrency.MaxRetries)
	}

	close(jobChannels.Jobs)
	wg.Wait()

	// Write failed types log
	utils.WriteFailedTypesLog()

	// Process and print timeout documents
	if len(timeoutContents) > 0 {
		retries := 0
		for retries < cfg.Concurrency.MaxRetries && len(timeoutContents) > 0 {
			wg.Wait() // Wait for all jobs to complete before next retry
			if err := processTimeoutContents(confluenceClient, &jobChannels); err != nil {
				log.Printf("error processing timeout documents (attempt %d/%d): %v",
					retries+1, cfg.Concurrency.MaxRetries, err)
			}
			retries++
		}
		if len(timeoutContents) > 0 {
			log.Printf("failed to process all timeout documents after %d attempts",
				cfg.Concurrency.MaxRetries)
		}
	}

	// Log any documents that resulted in a 404 error during status check
	batchPool.LogNotFoundErrors()
}

// processTimeoutContents processes all timeout documents by sending them to processContentOperation
func processTimeoutContents(confluenceClient *confluence.Client, jobChan *JobChannels) error {
	// Create a copy of space keys to safely iterate
	spaceKeys := make([]string, 0, len(timeoutContents))
	for spaceKey := range timeoutContents {
		spaceKeys = append(spaceKeys, spaceKey)
	}

	for _, spaceKey := range spaceKeys {
		contents := timeoutContents[spaceKey]
		client, exists := difyClients[spaceKey]
		if !exists {
			return fmt.Errorf("no Dify client for space %s", spaceKey)
		}

		// Create a copy of content IDs to safely iterate
		contentIDs := make([]string, 0, len(contents))
		for contentID := range contents {
			contentIDs = append(contentIDs, contentID)
		}

		for _, contentID := range contentIDs {
			operation := contents[contentID]
			if err := processContentOperation(contentID, operation, spaceKey, client, confluenceClient, jobChan); err != nil {
				return fmt.Errorf("failed to process timeout document %s: %w", contentID, err)
			}
			// Remove successfully processed content
			delete(contents, contentID)
		}

		// If all contents for this space are processed, remove the space entry
		if len(contents) == 0 {
			delete(timeoutContents, spaceKey)
		}
	}
	return nil
}

// statusChecker checks the status of a batch using Dify client
func statusChecker(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client, exists := difyClients[spaceKey]
	if !exists {
		return "", fmt.Errorf("no Dify client for space %s", spaceKey)
	}

	status, err := client.GetIndexingStatus(ctx, spaceKey, batch)
	if err != nil {
		return "", err
	}
	if len(status.Data) > 0 {
		if status.Data[0].IndexingStatus == "completed" {
			return "completed", nil
		}
		// Check if ProcessingStartedAt is more than 2 minutes ago
		processingStartedAt := time.Unix(int64(status.Data[0].ProcessingStartedAt), 0)
		if time.Since(processingStartedAt) > time.Duration(cfg.Concurrency.IndexingTimeout)*time.Minute {
			// Delete the document
			err := client.DeleteDocument(ctx, status.Data[0].ID)
			if err != nil {
				return "", fmt.Errorf("failed to delete timeout document for %s content %s: %w", spaceKey, title, err)
			}

			// Store and log the ID
			if op.Action == 1 {
				op.Action = 0
			}
			if timeoutContents[spaceKey] == nil {
				timeoutContents[spaceKey] = make(map[string]confluence.ContentOperation)
			}
			timeoutContents[spaceKey][status.Data[0].ID] = op

			return "deleted", nil
		}
		return status.Data[0].IndexingStatus, nil
	}

	return "", fmt.Errorf("no status data found")
}
