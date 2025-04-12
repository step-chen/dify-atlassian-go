package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	// "github.com/step-chen/dify-atlassian-go/internal/concurrency" // No longer needed here
	// Import the parent config package
	localfolder_cfg "github.com/step-chen/dify-atlassian-go/internal/config/localfolder" // Use the specific config package
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	difyClients   map[string]*dify.Client             // Map datasetID to Dify client
	batchPool     *BatchPool                          // Use BatchPool defined in this package
	timeoutFiles  map[string]map[string]FileOperation // Stores map[datasetID]map[filePath]FileOperation
	timeoutMutex  sync.Mutex                          // Mutex for protecting timeoutFiles
	cfg           *localfolder_cfg.Config             // Use the specific config type
	supportedExts map[string]bool                     // Map of supported file extensions
	maxFileSize   int64                               // Maximum file size in bytes
)

// FileOperation holds information about a file being processed
type FileOperation struct {
	Action   int       // 0: Create/Update, 1: Needs Retry (e.g., after timeout deletion)
	StartAt  time.Time // Timestamp when processing started
	DifyID   string    // Dify document ID if known
	FilePath string    // Original file path
	Title    string    // Document title (e.g., filename)
}

// Application entry point, handles initialization and task processing for local folders
func main() {
	// Load config file
	var err error
	// Load config relative to the executable's location or CWD
	cfg, err = localfolder_cfg.LoadConfig("config.yaml") // Use the specific loader
	if err != nil {
		log.Fatalf("Failed to load config 'config.yaml': %v", err)
	}

	// Initialize required tools (if any specific to local files are needed)
	utils.InitRequiredTools() // Keep this for now, might contain general utils

	// Init timeout files map
	timeoutFiles = make(map[string]map[string]FileOperation)

	// Init batch pool (using the one defined in this package)
	batchPool = NewBatchPool(cfg.Concurrency.BatchPoolSize, cfg.Concurrency.QueueSize, statusChecker, &cfg.BaseConfig) // Pass pointer to BaseConfig

	// Init Dify clients per configured folder/dataset
	difyClients = make(map[string]*dify.Client)
	for folderPath, dataset := range cfg.Dify.Datasets {
		client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, dataset, cfg)
		if err != nil {
			log.Fatalf("failed to create Dify client for Content dataset %s (folder %s): %v", dataset, folderPath, err)
		}
		if err = client.InitMetadata(); err != nil {
			log.Fatalf("failed to initialize metadata for Content dataset %s (folder %s): %v", dataset, folderPath, err)
		}
		difyClients[folderPath] = client
	}

	// Populate supported extensions map and max file size
	supportedExts = make(map[string]bool)
	for _, ext := range cfg.LocalFolder.SupportedExtensions {
		supportedExts[ext] = true
	}
	maxFileSize = cfg.LocalFolder.MaxFileSizeMB * 1024 * 1024 // Convert MB to bytes

	// Create worker pool with queue size
	jobChannels := JobChannels{
		Jobs: make(chan Job, cfg.Concurrency.QueueSize),
	}
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < cfg.Concurrency.Workers; i++ {
		wg.Add(1)
		go worker(jobChannels.Jobs, &wg) // worker function needs to be defined/adapted
	}

	// --- Main Processing Loop ---
	// This loop structure might need significant changes compared to Confluence
	// We need to iterate through folders, walk the file system, and handle retries for timed-out files.

	for i := 0; i < cfg.Concurrency.MaxRetries+1; i++ {
		currentTimeout := time.Duration((i+1)*cfg.Concurrency.IndexingTimeout) * time.Minute // Calculate current timeout
		log.Printf("Starting processing cycle %d with timeout %v", i+1, currentTimeout)

		// Process all configured folders
		for folderPath, dataset := range cfg.Dify.Datasets {
			// Use the Content dataset ID when processing the folder
			difyClient := difyClients[folderPath] // Assumes client was stored with Content ID

			if difyClient == nil {
				log.Printf("Warning: No Dify client found for Content dataset %s (folder %s). Skipping folder.", dataset, folderPath)
				continue
			}

			if err := processFolder(folderPath, dataset, difyClient, &jobChannels); err != nil {
				log.Printf("error processing folder %s (dataset %s): %v", folderPath, dataset, err)
			}
		}

		// Wait for batch monitoring tasks from the current cycle to complete
		log.Printf("Waiting for batch monitoring tasks (cycle %d) to complete...", i+1)
		batchPool.Wait()
		log.Printf("Batch monitoring complete (cycle %d).", i+1)

		// Check if there are files to retry
		timeoutMutex.Lock()
		filesToRetry := make(map[string]map[string]FileOperation)
		for dsID, files := range timeoutFiles {
			if len(files) > 0 {
				filesToRetry[dsID] = make(map[string]FileOperation)
				for fPath, op := range files {
					filesToRetry[dsID][fPath] = op
					log.Printf("File marked for retry: %s (Dataset: %s, DifyID: %s)", fPath, dsID, op.DifyID)
				}
			}
		}
		timeoutFiles = make(map[string]map[string]FileOperation) // Clear for the next cycle or final exit
		timeoutMutex.Unlock()

		if len(filesToRetry) == 0 {
			log.Println("No files marked for retry. Exiting processing loop.")
			break // Exit the retry loop if no timeouts occurred
		}

		// Resubmit timed-out files (if any) for the next cycle
		log.Printf("Resubmitting %d datasets with timed-out files for retry cycle %d", len(filesToRetry), i+2)
		for datasetID, files := range filesToRetry {
			difyClient := difyClients[datasetID]
			if difyClient == nil {
				log.Printf("Warning: No Dify client found for dataset %s during retry. Skipping.", datasetID)
				continue
			}
			for filePath, operation := range files {
				// Resubmit the job for the file that timed out
				log.Printf("Retrying file: %s", filePath)
				job := Job{
					DatasetID:  datasetID,
					FilePath:   filePath,
					Operation:  operation, // Pass the operation details including DifyID if deleted
					DifyClient: difyClient,
				}
				jobChannels.Jobs <- job
			}
		}

		if i == cfg.Concurrency.MaxRetries {
			log.Printf("Max retries (%d) reached. Some files might not have been indexed successfully.", cfg.Concurrency.MaxRetries)
		}
	} // --- End of Retry Loop ---

	// Close the job channel after all processing cycles (including retries) are done
	log.Println("Closing job channel...")
	close(jobChannels.Jobs)

	// Wait for all worker goroutines to finish processing ALL jobs
	log.Println("Waiting for all workers to complete...")
	wg.Wait()
	log.Println("All workers finished.")

	// Log failed types (if applicable, maybe adapt for file types)
	utils.WriteFailedTypesLog() // Keep or adapt this logging

	// Close the batch pool gracefully
	batchPool.Close()

	log.Println("Processing complete.")
}

// processFolder walks through a directory and submits jobs for supported files.
func processFolder(folderPath, datasetID string, client *dify.Client, jobChannels *JobChannels) error {
	log.Printf("Processing folder: %s for dataset: %s", folderPath, datasetID)

	// Get existing documents metadata from Dify to check for updates
	existingDocs, err := client.GetAllDocumentsMetadata()
	if err != nil {
		log.Printf("Warning: Failed to get existing documents metadata for dataset %s: %v. Proceeding without update checks.", datasetID, err)
		// Corrected type here:
		existingDocs = make(map[string]dify.LocalFileMetadata) // Continue with an empty map
	}

	processedFiles := make(map[string]bool) // Keep track of files processed in this walk

	err = filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return err
		}

		// Skip directories, hidden files/folders (starting with '.')
		if info.IsDir() || filepath.Base(path)[0] == '.' {
			if info.IsDir() && filepath.Base(path)[0] == '.' {
				log.Printf("Skipping hidden directory: %s", path)
				return filepath.SkipDir // Skip hidden directories entirely
			}
			// log.Printf("Skipping directory or hidden file: %s", path)
			return nil // Skip other directories and hidden files
		}

		// Check file extension
		ext := filepath.Ext(path)
		if !supportedExts[ext] {
			// log.Printf("Skipping unsupported file type: %s", path)
			return nil
		}

		// Check file size
		if info.Size() > maxFileSize {
			log.Printf("Skipping large file (> %d MB): %s (%d bytes)", cfg.LocalFolder.MaxFileSizeMB, path, info.Size())
			return nil
		}

		// Mark file as processed in this walk
		processedFiles[path] = true

		// Check if file needs update based on metadata (e.g., mod time)
		docID := utils.GenerateDocID(path) // Use file path to generate a unique ID
		existingMeta, exists := existingDocs[docID]
		needsUpdate := true // Assume update needed unless proven otherwise

		if exists {
			// Compare modification times
			localModTime := info.ModTime().UTC()
			difyModTime := existingMeta.LastModified // Assuming metadata stores this
			// Add a small buffer (e.g., 1 second) to handle potential precision differences
			if !localModTime.After(difyModTime.Add(time.Second)) {
				needsUpdate = false
				// log.Printf("Skipping unchanged file: %s", path)
			} else {
				log.Printf("File changed, scheduling update: %s (Local: %s, Dify: %s)", path, localModTime, difyModTime)
			}
		} else {
			log.Printf("New file detected, scheduling creation: %s", path)
		}

		if needsUpdate {
			job := Job{
				DatasetID:  datasetID,
				FilePath:   path,
				Operation:  FileOperation{Action: 0}, // 0 for Create/Update initially
				DifyClient: client,
			}
			jobChannels.Jobs <- job
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking the path %q: %w", folderPath, err)
	}

	// After walking, check for files in Dify metadata that were NOT found locally (means they were deleted)
	for docID, meta := range existingDocs {
		// We need the original file path from metadata to check against processedFiles
		originalPath := meta.OriginalPath // Assuming metadata stores this
		if originalPath == "" {
			log.Printf("Warning: Missing original path in metadata for docID %s (Dify ID: %s). Cannot check for deletion.", docID, meta.DifyDocumentID)
			continue
		}
		// Important: Ensure the path from metadata belongs to the *current* folder being processed.
		// This prevents deleting a file from dataset A just because it wasn't found while processing folder B.
		relPath, err := filepath.Rel(folderPath, originalPath)
		if err != nil || filepath.IsAbs(relPath) || relPath[0:2] == ".." {
			// If originalPath is not within folderPath, skip it.
			continue
		}

		if !processedFiles[originalPath] {
			log.Printf("File deleted locally, scheduling deletion in Dify: %s (DocID: %s, DifyID: %s)", originalPath, docID, meta.DifyDocumentID)
			// Submit a deletion job
			job := Job{
				DatasetID:      datasetID,
				FilePath:       originalPath,             // Use the path from metadata for deletion context
				Operation:      FileOperation{Action: 2}, // 2 for Delete
				DifyClient:     client,
				DifyDocumentID: meta.DifyDocumentID, // Pass the Dify ID for deletion
			}
			jobChannels.Jobs <- job
		}
	}

	return nil
}

// Check batch status using Dify client for local files
// Receives context, datasetID, filePath, title, batch ID, and the operation details.
func statusChecker(ctx context.Context, datasetID, filePath, title, batch string, op FileOperation) (string, error) {
	// Check for context cancellation first
	select {
	case <-ctx.Done():
		log.Printf("Context cancelled for batch %s (File: %s)", batch, filePath)
		return "", ctx.Err()
	default:
	}

	client, exists := difyClients[datasetID]
	if !exists {
		return "", fmt.Errorf("no Dify client for dataset %s", datasetID)
	}

	status, err := client.GetIndexingStatus(datasetID, batch) // Use datasetID here
	if err != nil {
		// Handle 404 as potentially completed (batch info might be cleaned up)
		if err.Error() == "unexpected status code: 404" {
			log.Printf("Batch %s not found for file %s (Dataset: %s), assuming completed.", batch, filePath, datasetID)
			return "completed", nil
		}
		log.Printf("Error getting indexing status for batch %s (File: %s, Dataset: %s): %v", batch, filePath, datasetID, err)
		return "", err // Return error for other issues
	}

	if len(status.Data) > 0 {
		currentStatus := status.Data[0].IndexingStatus
		if currentStatus == "completed" {
			return "completed", nil
		}

		// Check for timeout based on the *current* cycle's timeout value
		// We need access to the current timeout duration. This might require passing it via context or another mechanism.
		// For now, let's use the config's base timeout, but this needs refinement for retries.
		// A better approach might be to pass the specific timeout duration for this check via the BatchPool task context.
		currentTimeoutDuration := time.Duration(cfg.Concurrency.IndexingTimeout) * time.Minute // Simplified: Needs adjustment for retries

		processingStartTime := status.LastStepAt() // Use the last step time as the effective start for timeout check
		if time.Since(processingStartTime) > currentTimeoutDuration {
			log.Printf("Indexing timeout detected for batch %s (File: %s, Dataset: %s). Started: %s, Timeout: %v",
				batch, filePath, datasetID, processingStartTime, currentTimeoutDuration)

			if cfg.Concurrency.DeleteTimeoutContent {
				difyDocID := status.Data[0].ID
				log.Printf("Attempting to delete timed-out Dify document %s for file %s", difyDocID, filePath)
				// Use file path to generate the unique ID for deletion metadata tracking
				docID := utils.GenerateDocID(filePath)
				err := client.DeleteDocument(difyDocID, docID) // Pass docID for metadata update
				if err != nil {
					log.Printf("Failed to delete/update timed-out document %s for file %s: %v", difyDocID, filePath, err)
					// Decide if we should return error or mark as retry anyway
					// Returning error might stop the batch pool worker for this task
					return "", fmt.Errorf("failed to delete/update timeout document %s: %w", difyDocID, err)
				}
				log.Printf("Successfully deleted/updated timed-out document %s for file %s", difyDocID, filePath)

				// Store file info for retry
				timeoutMutex.Lock()
				if timeoutFiles[datasetID] == nil {
					timeoutFiles[datasetID] = make(map[string]FileOperation)
				}
				retryOp := op              // Copy the operation details
				retryOp.Action = 1         // Mark as needing retry
				retryOp.DifyID = difyDocID // Store the Dify ID that was deleted
				timeoutFiles[datasetID][filePath] = retryOp
				timeoutMutex.Unlock()
				log.Printf("Marked file %s for retry.", filePath)

				return "deleted", nil // Indicate deletion occurred, leading to retry
			} else {
				log.Printf("Indexing timed out for %s, but configured not to delete. Marking as completed.", filePath)
				// If not deleting, treat timeout as completed to avoid infinite loops if the issue persists
				return "completed", nil
			}
		}
		// Return current status if not completed and not timed out
		return currentStatus, nil
	}

	// No status data found, which is unexpected if the batch ID is valid
	log.Printf("No status data found for batch %s (File: %s, Dataset: %s)", batch, filePath, datasetID)
	return "", fmt.Errorf("no status data found for batch %s", batch)
}

// --- TODO ---
// - Define Job and JobChannels structs (similar to confluence version but with FilePath etc.)
// - Implement the worker function to handle file reading and Dify API calls based on Job details.
// - Define config.LocalFolderConfig struct in internal/config/config.go
// - Implement config.LoadLocalFolderConfig in internal/config/config.go
// - Refine timeout handling in statusChecker, potentially passing timeout duration via context.
// - Implement utils.GenerateDocID (e.g., hash of relative path).
// - Ensure Dify metadata includes OriginalPath and LastModified.
