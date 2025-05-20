package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// processDirectory processes all files in a given directory
func processDirectory(dirKey, dirPath string, client *dify.Client, jobChan *JobChannels) error {
	// Get list of files
	files, err := getDirectoryFiles(dirPath)
	if err != nil {
		return fmt.Errorf("error getting files for directory %s: %w", dirPath, err)
	}

	// Initialize operations based on existing mappings
	if err := initOperations(client, files); err != nil {
		return fmt.Errorf("error initializing operations for directory %s: %w", dirPath, err)
	}

	batchPool.SetTotal(dirKey, len(files))

	// Process each file operation
	for filePath, operation := range files {
		if err := processOperation(filePath, operation, dirKey, client, jobChan); err != nil {
			log.Printf("error processing directory %s file %s: %v", dirPath, filePath, err)
		}
	}

	log.Printf("=========================================================")
	log.Printf("All operations for directory %s have been processed.", dirPath)
	log.Printf("=========================================================")

	return nil
}

// getDirectoryFiles retrieves all files in a directory that match allowed types
func getDirectoryFiles(dirPath string) (map[string]batchpool.Operation, error) {
	files := make(map[string]batchpool.Operation)

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Check file type using MIMEType
		mimeType := utils.GetMIMEType(path)
		if cfg.AllowedTypes != nil && !cfg.AllowedTypes[mimeType] {
			return nil // Skip unsupported types
		}
		if cfg.UnsupportedTypes != nil && cfg.UnsupportedTypes[mimeType] {
			return nil // Skip explicitly unsupported types
		}

		// Get file modification time
		modTime := info.ModTime().Format(time.RFC3339)

		files[path] = batchpool.Operation{
			Action:           0, // Create by default
			LastModifiedDate: modTime,
		}

		return nil
	})

	return files, err
}

// initOperations initializes file operations based on existing Dify mappings
func initOperations(client *dify.Client, files map[string]batchpool.Operation) error {
	// Fetch existing documents
	filePathToDifyRecord, err := client.FetchDocumentsList(0, 100)
	if err != nil {
		return fmt.Errorf("failed to list documents for dataset %s: %v", client.DatasetID(), err)
	}

	// Iterate over existing records
	for filePath, record := range filePathToDifyRecord {
		if op, ok := files[filePath]; !ok {
			// Add delete operation for unmapped files
			files[filePath] = batchpool.Operation{
				Action:    2, // Delete
				DifyID:    record.DifyID,
				DatasetID: client.DatasetID(),
			}
		} else {
			// Update existing operation
			op.DifyID = record.DifyID
			op.DatasetID = client.DatasetID()

			// Compare modification times
			equal := !utils.BeforeRFC3339Times(record.When, op.LastModifiedDate)

			// Determine action based on time comparison
			if !equal {
				op.Action = 1 // Update if times differ
			} else {
				// Skip if no action needed
				delete(files, filePath)
				continue
			}

			files[filePath] = op
		}
	}

	return nil
}

// processOperation handles individual file operations
func processOperation(filePath string, operation batchpool.Operation, dirKey string, client *dify.Client, jobChan *JobChannels) error {
	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Convert content to markdown format
	markdownContent, err := utils.PrepareLocalFileMarkdown(filePath)
	if err != nil {
		log.Printf("warning: failed to convert file %s to markdown: %v", filePath, err)
		// Fallback to original content if conversion fails
		markdownContent = string(content)
	}

	j := Job{
		Type:       operation.Type,
		DocumentID: operation.DifyID,
		FilePath:   filePath,
		Content:    markdownContent,
		Client:     client,
		Op:         operation,
		DirKey:     dirKey,
	}

	jobChan.Jobs <- j
	return nil
}

// createDocument creates a new document in Dify
func createDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Create new document
	docRequest := dify.CreateDocumentRequest{
		Name:              filepath.Base(j.FilePath),
		Text:              string(j.Content),
		IndexingTechnique: cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:           cfg.Dify.RagSetting.DocForm,
	}

	resp, err := j.Client.CreateDocumentByText(&docRequest)

	if err != nil {
		log.Printf("failed to create Dify document for directory %s file %s: %v", j.DirKey, j.FilePath, err)
		return err
	}

	j.Op.DifyID = resp.Document.ID
	j.Op.DatasetID = j.Client.DatasetID()
	j.Op.StartAt = time.Now()
	j.Client.SetHashMapping(j.Client.GetHashByDifyIDFromRecord(resp.Document.ID), j.Op.DifyID)

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		URL:        j.FilePath,
		SourceType: "directory",
		Type:       "file",
		When:       time.Now().Format(time.RFC3339),
		Xxh3:       j.Client.GetHashByDifyIDFromRecord(resp.Document.ID),
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, params); err != nil {
		// Pass file path during cleanup deletion attempt
		if errDel := j.Client.DeleteDocument(resp.Document.ID, j.FilePath); errDel != nil {
			log.Printf("failed to delete/update Dify document %s after metadata update failure: %v", resp.Document.ID, errDel)
		}
		return err
	}

	// Add document to batch pool for indexing tracking
	err = batchPool.Add(context.Background(), j.DirKey, j.FilePath, docRequest.Name, resp.Batch, j.Op)
	if err != nil {
		// Log error if adding to the pool fails (e.g., pool shutdown)
		log.Printf("Error adding task to batch pool for directory %s file %s: %v", j.DirKey, j.FilePath, err)
		// Consider how to handle this - should the document be deleted? For now, just log.
	}

	return nil // Return nil even if adding to pool failed, as document creation succeeded
}

// updateDocument updates an existing document in Dify
func updateDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Update document
	updateRequest := dify.UpdateDocumentRequest{
		Name: filepath.Base(j.FilePath),
		Text: string(j.Content),
	}

	resp, err := j.Client.UpdateDocumentByText(j.DocumentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for directory %s file %s: %v", j.DirKey, j.FilePath, err)
		return err
	}

	j.Op.StartAt = time.Now()

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		URL:        j.FilePath,
		SourceType: "directory",
		Type:       "file",
		When:       time.Now().Format(time.RFC3339),
		Xxh3:       j.Client.GetHashByDifyIDFromRecord(j.DocumentID),
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, params); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	err = batchPool.Add(context.Background(), j.DirKey, j.FilePath, updateRequest.Name, resp.Batch, j.Op)
	if err != nil {
		log.Printf("Error adding task to batch pool for directory %s file %s: %v", j.DirKey, j.FilePath, err)
	}

	return nil // Return nil even if adding to pool failed, as document update succeeded
}

// deleteDocument deletes a document from Dify
func deleteDocument(j *Job) error {
	// Determine the file path based on the job type
	var filePath string
	if j.Content != "" {
		filePath = j.FilePath
	} else {
		// Should not happen if job is constructed correctly
		log.Printf("Error: Could not determine file path for delete job with Dify ID %s", j.DocumentID)
		// Fallback: attempt deletion without specific file path? Or return error?
		// Let's return an error as the DeleteDocument logic now relies on it.
		return fmt.Errorf("could not determine file path for delete operation on Dify document %s", j.DocumentID)
	}

	// Delete document or update metadata
	err := j.Client.DeleteDocument(j.DocumentID, filePath)
	if err != nil {
		log.Printf("failed to delete/update Dify document %s (for file path %s): %v", j.DocumentID, filePath, err)
		// Still return the error if deletion/update failed
		return err
	}

	// Log deletion success. ProgressString still works for getting current progress.
	log.Printf("%s Successfully deleted Dify document: %s", batchPool.ProgressString(j.DirKey), j.DocumentID)
	// Note: Since deletion doesn't involve batch monitoring, it completes immediately.
	// The BatchPool's total count for the directory (set via SetTotal) should account for this.
	// If SetTotal counts only items needing monitoring, deletions shouldn't affect its count.
	// If SetTotal counts *all* operations (create/update/delete), then the BatchPool's
	// completed count won't reach the total unless deletions are also marked complete somehow.
	// Let's assume SetTotal counts only monitorable tasks (create/update/upload).
	return nil
}
