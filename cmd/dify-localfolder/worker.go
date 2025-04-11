package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// Job represents a task for processing a single local file.
type Job struct {
	DatasetID      string                  // Target Dify dataset ID
	FilePath       string                  // Absolute path to the local file
	Operation      FileOperation           // Details about the operation (Create/Update, Retry, Delete)
	DifyClient     *dify.Client            // Dify client instance for the dataset
	DifyDocumentID string                  // Dify document ID (used for updates/deletions/retries)
	ExistingMeta   *dify.LocalFileMetadata // Pointer to existing metadata from Dify, nil if not found
}

// JobChannels holds the channel for distributing jobs to workers.
type JobChannels struct {
	Jobs chan Job
}

// worker processes jobs from the job channel.
func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		log.Printf("Worker received job: Action=%d, File=%s, Dataset=%s", job.Operation.Action, job.FilePath, job.DatasetID)

		switch job.Operation.Action {
		case 0: // Create/Update
			fallthrough // Treat Retry (Action 1) the same as Create/Update initially
		case 1: // Retry (after timeout/deletion)
			// This function will handle reading the file, hashing, checking metadata,
			// and calling the appropriate Dify API (Create or Update).
			// It will also submit the batch monitoring task to the batchPool.
			if err := processCreateUpdateJob(&job); err != nil {
				log.Printf("Error processing create/update/retry job for file %s: %v", job.FilePath, err)
				// TODO: Consider adding failed jobs to a separate list or mechanism for final reporting.
			}
		case 2: // Delete
			// This function will call the Dify API to delete the document.
			if job.DifyDocumentID == "" {
				log.Printf("Warning: Delete action requested for file %s but no DifyDocumentID provided. Skipping.", job.FilePath)
				continue // Skip if no ID to delete
			}
			if err := processDeleteJob(&job); err != nil {
				log.Printf("Error processing delete job for file %s (DifyID: %s): %v", job.FilePath, job.DifyDocumentID, err)
				// TODO: Handle deletion errors (e.g., retry?)
			}
		default:
			log.Printf("Unknown job action type %d for file %s", job.Operation.Action, job.FilePath)
		}
	}
	log.Println("Worker finished.")
}

// processCreateUpdateJob handles reading a local file, comparing hashes/mod times,
// and calling the appropriate Dify API (Create or Update).
func processCreateUpdateJob(job *Job) error {
	filePath := job.FilePath
	log.Printf("Processing Create/Update/Retry for: %s (Dataset: %s)", filePath, job.DatasetID)

	// 1. Get File Info & Read Content
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File %s does not exist (possibly deleted after initial scan). Skipping job.", filePath)
			// If the file doesn't exist, we might need to trigger a delete if it was previously indexed.
			// However, the main loop's deletion check should handle this. We just skip the current job.
			return nil
		}
		return fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	}
	localModTime := fileInfo.ModTime().UTC()

	contentBytes, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	content := string(contentBytes)

	// 2. Generate IDs and Hashes
	docID := utils.GenerateDocID(filePath) // Our internal ID based on path
	contentHash := utils.CalculateXXH3FromBytes(contentBytes)
	fileName := filepath.Base(filePath)

	// 3. Check if Update is Needed
	needsAPICall := true
	isUpdate := false
	difyDocIDToUpdate := "" // Dify's internal ID for the document

	if job.ExistingMeta != nil {
		isUpdate = true
		difyDocIDToUpdate = job.ExistingMeta.DifyDocumentID
		// Compare content hash first
		if contentHash == job.ExistingMeta.ContentHash {
			// Hashes match, check modification time as a fallback (though hash is better)
			// Add a buffer for time comparison
			if !localModTime.After(job.ExistingMeta.LastModified.Add(time.Second)) {
				log.Printf("Skipping unchanged file (hash match): %s", filePath)
				needsAPICall = false
			} else {
				log.Printf("File hash matches but mod time is newer for %s. Proceeding with update.", filePath)
			}
		} else {
			log.Printf("File content hash changed for %s. Proceeding with update.", filePath)
		}
	} else {
		// No existing metadata, definitely needs creation
		log.Printf("New file detected: %s. Proceeding with creation.", filePath)
	}

	if !needsAPICall {
		return nil // No API call needed
	}

	// 4. Prepare Metadata Payload for Dify API
	// We need a way to construct the metadata list expected by Dify API calls.
	// Let's assume a helper function in the dify client or utils package.
	// This needs the configured metadata field IDs from the client.
	metadataPayload, err := job.DifyClient.BuildLocalFileMetadataPayload(docID, filePath, localModTime, contentHash)
	if err != nil {
		return fmt.Errorf("failed to build metadata payload for %s: %w", filePath, err)
	}

	// 5. Call Dify API
	var apiResp *dify.CreateDocumentResponse
	var apiErr error

	// Use config from the client
	difyCfg := job.DifyClient.GetConfig().GetDifyConfig() // Assuming GetConfig() exists

	if isUpdate {
		log.Printf("Calling UpdateDocumentByText for %s (DifyID: %s)", filePath, difyDocIDToUpdate)
		updateReq := &dify.UpdateDocumentRequest{
			Name: fileName,
			Text: content,
			// ProcessRule can be omitted to use dataset defaults or set explicitly
			// ProcessRule: dify.DefaultProcessRule(job.DifyClient.GetConfig()),
			// METADATA IS UPDATED IN A SEPARATE CALL LATER
		}
		apiResp, apiErr = job.DifyClient.UpdateDocumentByText(job.DatasetID, difyDocIDToUpdate, updateReq)
	} else {
		log.Printf("Calling CreateDocumentByText for %s", filePath)
		createReq := &dify.CreateDocumentRequest{
			Name:              fileName,
			Text:              content,
			IndexingTechnique: difyCfg.RagSetting.IndexingTechnique,                // Get from config
			DocForm:           difyCfg.RagSetting.DocForm,                          // Get from config
			DocLanguage:       "English",                                           // TODO: Make configurable?
			ProcessRule:       dify.DefaultProcessRule(job.DifyClient.GetConfig()), // Use default/config rules
			// METADATA IS UPDATED IN A SEPARATE CALL LATER
		}
		apiResp, apiErr = job.DifyClient.CreateDocumentByText(createReq)
	}

	// 6. Handle API Response
	if apiErr != nil {
		// TODO: Implement more sophisticated error handling (e.g., retry specific errors?)
		return fmt.Errorf("dify API call failed for %s: %w", filePath, apiErr)
	}

	if apiResp == nil {
		return fmt.Errorf("dify API call returned nil response for %s", filePath)
	}

	difyDocID := apiResp.Document.ID
	log.Printf("Dify API call successful for %s. DifyDocID: %s, Batch: %s", filePath, difyDocID, apiResp.Batch)

	// 7. Update Metadata (Separate API Call)
	if err := job.DifyClient.UpdateMetadataForDocument(difyDocID, metadataPayload); err != nil {
		// Log error but don't fail the entire job just for metadata update failure
		log.Printf("Warning: Failed to update metadata for document %s (DifyID: %s): %v", filePath, difyDocID, err)
	}

	// 8. Submit to Batch Pool if Batch ID exists
	if apiResp.Batch != "" {
		// Use the operation details from the original job
		batchPool.Submit(job.DatasetID, filePath, fileName, apiResp.Batch, job.Operation)
		log.Printf("Submitted batch %s to monitor for file %s", apiResp.Batch, filePath)
	} else {
		log.Printf("No batch ID returned for %s, assuming immediate completion or error.", filePath)
		// If no batch ID, we might assume it's done or failed. Check apiResp.Document.IndexingStatus?
		// For now, we just don't submit to the pool.
	}

	// 9. Update Client's Internal Metadata Cache (Important!)
	// This ensures subsequent checks use the latest known state.
	// Use the difyDocID obtained from the create/update response.
	err = job.DifyClient.UpdateLocalFileMetadataCache(dify.LocalFileMetadata{
		DifyDocumentID: difyDocID,
		DocID:          docID,
		OriginalPath:   filePath,
		LastModified:   localModTime,
		ContentHash:    contentHash,
	})
	if err != nil {
		// Log error but don't fail the job just for a cache update issue
		log.Printf("Warning: Failed to update Dify client metadata cache for %s: %v", filePath, err)
	}

	return nil // Job processed successfully
}

// processDeleteJob handles calling the Dify API to delete a document.
func processDeleteJob(job *Job) error {
	filePath := job.FilePath
	difyDocID := job.DifyDocumentID
	log.Printf("Processing Delete for: %s (DifyID: %s, Dataset: %s)", filePath, difyDocID, job.DatasetID)

	// Generate the internal docID needed for the DeleteDocument method's cache cleanup
	docID := utils.GenerateDocID(filePath)

	// Call Dify API to delete the document
	// The DeleteDocument method handles cache cleanup internally on success.
	err := job.DifyClient.DeleteDocument(difyDocID, docID) // Pass Dify ID and our internal docID
	if err != nil {
		// Specific handling for "already deleted" might be done within DeleteDocument itself (returning nil)
		log.Printf("Failed to delete document %s (DifyID: %s) via Dify API: %v", filePath, difyDocID, err)
		return fmt.Errorf("dify API delete call failed for %s: %w", filePath, err)
	}

	log.Printf("Successfully processed delete request for %s (DifyID: %s)", filePath, difyDocID)
	return nil // Deletion processed successfully
}
