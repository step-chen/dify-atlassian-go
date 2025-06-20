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

// generateDocID creates a unique identifier for a file within a repo.
func generateDocID(repoKey, filePath string) string {
	// Using a simple combination. Ensure this is consistent with how IDs are generated elsewhere (e.g., in main.go).
	return fmt.Sprintf("%s:%s", repoKey, filePath)
}

// createDocument handles creating a new document in Dify based on a local file.
func createDocument(job *Job) error {
	fullPath := filepath.Join(job.RepoPath, job.FilePath)
	log.Printf("Creating document for file: %s", fullPath)

	contentBytes, err := os.ReadFile(fullPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", fullPath, err)
	}
	content := string(contentBytes)
	// Use CalculateXXH3FromBytes which takes bytes and returns string
	hash := utils.XXH3FromBytes(contentBytes)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return fmt.Errorf("failed to get file info for %s: %w", fullPath, err)
	}
	modTime := fileInfo.ModTime()

	// Generate the unique document ID for this file
	docID := generateDocID(job.RepoKey, job.FilePath)

	// Build metadata payload using the client's helper
	metadataPayload, err := job.Client.BuildLocalFileMetadataPayload(docID, job.FilePath, modTime, hash)
	if err != nil {
		return fmt.Errorf("failed to build metadata payload for %s: %w", job.FilePath, err)
	}

	// Prepare the request for Dify (Metadata/OriginalID not supported here)
	createReq := &dify.CreateDocumentRequest{
		// Correct path for IndexingTechnique
		IndexingTechnique: job.Client.GetConfig().GetDifyConfig().RagSetting.IndexingTechnique,
		Name:              job.FilePath, // Use relative path as name
		Text:              content,
		// OriginalID and Metadata are not fields in CreateDocumentRequest
		ProcessRule: dify.DefaultProcessRule(job.Client.GetConfig()), // Use default rule helper
		// DocForm:           "text", // Default is text
		// DocLanguage: // Optional: Could be set from config if needed
		// CreatedBy: // Optional
	}

	// Call Dify API
	resp, err := job.Client.CreateDocumentByText(createReq, nil)
	if err != nil {
		return fmt.Errorf("failed to create document in Dify for %s: %w", job.FilePath, err)
	}
	newDifyID := resp.Document.ID
	log.Printf("Successfully submitted create request for %s. Dify Document ID: %s, Batch: %s", job.FilePath, newDifyID, resp.Batch)

	// --- Update Metadata Separately ---
	log.Printf("Updating metadata for new document %s", newDifyID)
	err = job.Client.UpdateMetadataForDocument(newDifyID, metadataPayload)
	if err != nil {
		// Log error but don't fail the job just because metadata update failed initially
		log.Printf("Warning: Failed to update metadata for newly created document %s (File: %s): %v", newDifyID, job.FilePath, err)
		// Consider retrying metadata update later or marking for check
	} else {
		log.Printf("Successfully updated metadata for new document %s", newDifyID)
	}
	// --- End Metadata Update ---

	// Update local cache with the new Dify ID and metadata
	localMeta := dify.LocalFileMetadata{
		DifyDocumentID: newDifyID, // Use the ID from the create response
		DocID:          docID,
		OriginalPath:   job.FilePath,
		LastModified:   modTime,
		ContentHash:    hash,
	}
	if err := job.Client.UpdateLocalFileMetadataCache(localMeta); err != nil {
		// Log error but don't fail the job just because cache update failed
		log.Printf("Warning: Failed to update local cache for created document %s (Dify ID: %s): %v", job.FilePath, resp.Document.ID, err)
	}

	// Add task to batch pool for monitoring
	job.Op.DifyID = newDifyID // Update operation with Dify ID
	job.Op.StartAt = time.Now()
	// Add to batch pool for monitoring
	err = batchPool.Add(context.Background(), job.RepoKey, docID, job.FilePath, resp.Batch, "", job.Op)
	if err != nil {
		log.Printf("Error adding CREATE task to batch pool for %s/%s (DifyID: %s): %v", job.RepoKey, job.FilePath, newDifyID, err)
		// Consider if the Dify document should be deleted if adding to pool fails.
	}
	return nil
}

// updateDocument handles updating an existing document in Dify.
func updateDocument(job *Job) error {
	fullPath := filepath.Join(job.RepoPath, job.FilePath)
	log.Printf("Updating document %s for file: %s", job.DocumentID, fullPath)

	contentBytes, err := os.ReadFile(fullPath)
	if err != nil {
		// If the file doesn't exist locally anymore, maybe it should be deleted in Dify?
		// For now, return error. Deletion logic should handle missing local files.
		return fmt.Errorf("failed to read file for update %s: %w", fullPath, err)
	}
	content := string(contentBytes)
	// Use CalculateXXH3FromBytes which takes bytes and returns string
	hash := utils.XXH3FromBytes(contentBytes)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return fmt.Errorf("failed to get file info for update %s: %w", fullPath, err)
	}
	modTime := fileInfo.ModTime()

	// Generate the unique document ID for this file
	docID := generateDocID(job.RepoKey, job.FilePath)

	// Build metadata payload
	metadataPayload, err := job.Client.BuildLocalFileMetadataPayload(docID, job.FilePath, modTime, hash)
	if err != nil {
		return fmt.Errorf("failed to build metadata payload for update %s: %w", job.FilePath, err)
	}

	// Prepare the update request (Metadata/OriginalID not supported here)
	updateReq := &dify.UpdateDocumentRequest{
		Name: job.FilePath, // Update name in case path changed (though ID is primary)
		Text: content,
		// OriginalID and Metadata are not fields in UpdateDocumentRequest
		ProcessRule: dify.DefaultProcessRule(job.Client.GetConfig()),
		// DocForm: "text", // Default
	}

	// Call Dify API
	resp, err := job.Client.UpdateDocumentByText(job.DocumentID, updateReq, batchpool.Keywords{})
	if err != nil {
		return fmt.Errorf("failed to update document %s in Dify for %s: %w", job.DocumentID, job.FilePath, err)
	}

	log.Printf("Successfully submitted update request for %s. Dify Document ID: %s, Batch: %s", job.FilePath, job.DocumentID, resp.Batch) // Update uses existing Dify ID

	// --- Update Metadata Separately ---
	log.Printf("Updating metadata for existing document %s", job.DocumentID)
	err = job.Client.UpdateMetadataForDocument(job.DocumentID, metadataPayload)
	if err != nil {
		// Log error but don't fail the job just because metadata update failed
		log.Printf("Warning: Failed to update metadata for existing document %s (File: %s): %v", job.DocumentID, job.FilePath, err)
	} else {
		log.Printf("Successfully updated metadata for existing document %s", job.DocumentID)
	}
	// --- End Metadata Update ---

	// Update local cache
	localMeta := dify.LocalFileMetadata{
		DifyDocumentID: job.DocumentID, // Use existing Dify ID
		DocID:          docID,
		OriginalPath:   job.FilePath,
		LastModified:   modTime,
		ContentHash:    hash,
	}
	if err := job.Client.UpdateLocalFileMetadataCache(localMeta); err != nil {
		log.Printf("Warning: Failed to update local cache for updated document %s (Dify ID: %s): %v", job.FilePath, job.DocumentID, err)
	}

	// Add task to batch pool for monitoring
	job.Op.DifyID = job.DocumentID // Use existing Dify ID
	job.Op.StartAt = time.Now()
	// Add to batch pool for monitoring
	err = batchPool.Add(context.Background(), job.RepoKey, docID, job.FilePath, resp.Batch, "", job.Op)
	if err != nil {
		log.Printf("Error adding UPDATE task to batch pool for %s/%s (DifyID: %s): %v", job.RepoKey, job.FilePath, job.DocumentID, err)
	}
	return nil
}

// deleteDocument handles deleting a document from Dify.
func deleteDocument(job *Job) error {
	log.Printf("Deleting document %s (associated with Repo: %s, File: %s)", job.DocumentID, job.RepoKey, job.FilePath)

	// Use the exported DeleteDocument method. Pass our unique docID as the 'confluenceIDToRemove'.
	// This should trigger the internal performDeleteRequest when it's the last ID.
	docID := generateDocID(job.RepoKey, job.FilePath)
	err := job.Client.DeleteDocument(job.DocumentID, "git", docID)
	if err != nil {
		// DeleteDocument handles logging internally, including 404s treated as success.
		return fmt.Errorf("failed to delete document %s from Dify (via DeleteDocument): %w", job.DocumentID, err)
	}

	log.Printf("Successfully deleted document %s from Dify (or it was already gone).", job.DocumentID)
	// Cache cleanup is handled within DeleteDocument/performDeleteRequest.
	// No need to add to batch pool for monitoring deletion status.

	return nil
}
