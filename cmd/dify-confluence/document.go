package main

import (
	"context" // Added context import
	"fmt"
	"log" // Added strings import
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// processSpaceOperations processes all operations for a given space using worker queues
func processSpace(spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels) error {
	// Get space contents
	contents, err := confluenceClient.GetSpaceContentsList(spaceKey)
	if err != nil {
		return fmt.Errorf("error getting contents for space %s: %w", spaceKey, err)
	}

	// Initialize operations based on existing mappings
	if err := initOperations(client, contents); err != nil {
		return fmt.Errorf("error initializing operations for space %s: %w", spaceKey, err)
	}

	batchPool.SetTotal(spaceKey, len(contents))

	// Process each content operation
	for contentID, operation := range contents {
		if err := processOperation(contentID, operation, spaceKey, client, confluenceClient, jobChan); err != nil {
			log.Printf("error processing space %s title %s media type %s: %v", spaceKey, contentID, operation.MediaType, err)
		}
	}

	log.Printf("=========================================================")
	log.Printf("All operations for space %s have been processed.", spaceKey)
	log.Printf("=========================================================")

	return nil
}

func initOperations(client *dify.Client, contents map[string]batchpool.Operation) error {
	// FetchDocumentsList now returns map[confluenceID]DifyDocumentMetadataRecord
	// and populates client.metaMapping internally (map[difyID]DifyDocumentMetadataRecord)
	confluenceIDToDifyRecord, err := client.FetchDocumentsList(0, 100)
	if err != nil {
		return fmt.Errorf("failed to list documents for dataset %s: %v", client.DatasetID(), err)
	}

	// Iterate over the fetched records (keyed by Confluence ID)
	for contentID, record := range confluenceIDToDifyRecord {
		if op, ok := contents[contentID]; !ok {
			// Add new operation for unmapped content
			contents[contentID] = batchpool.Operation{
				Action:    2, // Delete
				DifyID:    record.DifyID,
				DatasetID: client.DatasetID(),
			}
		} else {
			// Update existing operation
			op.DifyID = record.DifyID
			op.DatasetID = client.DatasetID()

			// Compare times using utility function (Confluence op vs Dify record)
			equal := !utils.BeforeRFC3339Times(record.When, op.LastModifiedDate)

			// Determine action based on time comparison
			if !equal {
				op.Action = 1 // Update if times differ
			} else {
				// Delete the operation since no action is needed
				delete(contents, contentID)
				continue
			}

			contents[contentID] = op
		}
	}

	return nil
}

// processOperation handles individual content operations based on type and action
func processOperation(contentID string, operation batchpool.Operation, spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels) error {
	j := Job{
		Type:             operation.Type,
		DocumentID:       operation.DifyID,
		SpaceKey:         spaceKey,
		Client:           client,
		ConfluenceClient: confluenceClient,
		Op:               operation,
	}

	var content *confluence.Content
	var err error

	switch operation.Type {
	case 0: // page
		content, err = confluenceClient.GetContent(contentID, cfg.Confluence.OnlyTitle)
		if err != nil {
			return fmt.Errorf("failed to get content %s: %w", contentID, err)
		}
	case 1: // attachment
		content, err = confluenceClient.GetAttachment(contentID, cfg.Confluence.OnlyTitle)
		if err != nil {
			return fmt.Errorf("failed to get attachment %s: %w", contentID, err)
		}
	default:
		// Handle unsupported content types
		return fmt.Errorf("unsupported content type %d for content ID %s", operation.Type, contentID)
	}

	if content.Content == "" {
		if operation.DifyID != "" {
			// Attempt to delete the document if it exists
			if err := client.DeleteDocument(operation.DifyID, "confluence", contentID); err != nil {
				log.Printf("failed to delete empty attachment %s: %v", contentID, err)
			}
		}
		batchPool.MarkTaskComplete(spaceKey)
		return nil
	}

	j.Content = content
	docID := client.GetDifyIDByHash(j.Content.Xxh3)
	if docID != "" {
		params := dify.DocumentMetadataRecord{
			URL:               j.Content.URL,
			SourceType:        "confluence", // Added SourceType
			Type:              j.Content.Type,
			SpaceKey:          j.SpaceKey,
			ConfluenceIDToAdd: j.Content.ID,          // Use the transient field to add this ID
			When:              j.Content.PublishDate, // Renamed from Timestamp
			Xxh3:              j.Content.Xxh3,        // Renamed from XXH3
			DifyID:            docID,                 // Added DifyID
		}

		if !j.Client.IsEqualDifyMeta(j.Content.ID, params) {
			// Update metadata using the new struct
			if err := j.Client.UpdateDocumentMetadata(docID, "confluence", params); err != nil {
				log.Printf("failed to update document metadata for %s: %v", docID, err)
			}
		}

		if j.DocumentID != docID && j.DocumentID != "" {
			j.Client.DeleteDocument(j.DocumentID, "confluence", j.Content.ID)
		}
		batchPool.MarkTaskComplete(j.SpaceKey)
		return nil
	} else {
		j.Op.Action = 0
	}

	jobChan.Jobs <- j
	return nil
}

func createDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Create new document
	docRequest := dify.CreateDocumentRequest{
		Name:              j.Content.Title,
		Text:              j.Content.Content,
		IndexingTechnique: cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:           cfg.Dify.RagSetting.DocForm,
	}

	resp, err := j.Client.CreateDocumentByText(&docRequest)

	if err != nil {
		log.Printf("failed to create Dify document for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		return err
	}

	j.Op.DifyID = resp.Document.ID
	j.Op.DatasetID = j.Client.DatasetID()
	j.Op.StartAt = time.Now()
	j.Client.SetHashMapping(j.Content.Xxh3, j.Op.DifyID)

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		URL:               j.Content.URL,
		SourceType:        "confluence", // Added SourceType
		Type:              j.Content.Type,
		SpaceKey:          j.SpaceKey,
		ConfluenceIDToAdd: j.Content.ID,          // Use the transient field to add this ID
		When:              j.Content.PublishDate, // Renamed from Timestamp
		Xxh3:              j.Content.Xxh3,        // Renamed from XXH3
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, "confluence", params); err != nil {
		// Pass Confluence ID (j.Content.ID) during cleanup deletion attempt
		if errDel := j.Client.DeleteDocument(resp.Document.ID, "confluence", j.Content.ID); errDel != nil {
			log.Printf("failed to delete/update Dify document %s after metadata update failure: %v", resp.Document.ID, errDel)
		}
		return err
	} // <--- Added missing closing brace

	// Add document to batch pool for indexing tracking
	// Add context.Background() as the first argument
	err = batchPool.Add(context.Background(), j.SpaceKey, j.Content.ID, j.Content.Title, resp.Batch, j.Op)
	if err != nil {
		// Log error if adding to the pool fails (e.g., pool shutdown)
		log.Printf("Error adding task to batch pool for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		// Consider how to handle this - should the document be deleted? For now, just log.
	}

	return nil // Return nil even if adding to pool failed, as document creation succeeded
}

func updateDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Update document
	updateRequest := dify.UpdateDocumentRequest{
		Name: j.Content.Title,
		Text: j.Content.Title,
	}

	resp, err := j.Client.UpdateDocumentByText(j.DocumentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		return err
	}

	j.Op.StartAt = time.Now()

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		URL:               j.Content.URL,
		SourceType:        "confluence", // Added SourceType
		Type:              j.Content.Type,
		SpaceKey:          j.SpaceKey,
		ConfluenceIDToAdd: j.Content.ID,          // Use the transient field to add this ID
		When:              j.Content.PublishDate, // Renamed from Timestamp
		Xxh3:              j.Content.Xxh3,        // Renamed from XXH3
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, "confluence", params); err != nil {
		return err
	} // <--- Added missing closing brace

	// Add document to batch pool for indexing tracking
	// Add context.Background() as the first argument
	err = batchPool.Add(context.Background(), j.SpaceKey, j.Content.ID, j.Content.Title, resp.Batch, j.Op)
	if err != nil {
		log.Printf("Error adding task to batch pool for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
	}

	return nil // Return nil even if adding to pool failed, as document update succeeded
}

func deleteDocument(j *Job) error {
	// Determine the Confluence ID based on the job type
	var confluenceID string
	if j.Type == batchpool.Page && j.Content != nil { // Changed JobTypePage to ContentTypePage
		confluenceID = j.Content.ID
	} else {
		// Should not happen if job is constructed correctly
		log.Printf("Error: Could not determine Confluence ID for delete job with Dify ID %s", j.DocumentID)
		// Fallback: attempt deletion without specific confluence ID? Or return error?
		// Let's return an error as the DeleteDocument logic now relies on it.
		return fmt.Errorf("could not determine confluence ID for delete operation on Dify document %s", j.DocumentID)
	}

	// Delete document or update metadata
	err := j.Client.DeleteDocument(j.DocumentID, "confluence", confluenceID)
	if err != nil {
		log.Printf("failed to delete/update Dify document %s (for Confluence ID %s): %v", j.DocumentID, confluenceID, err)
		// Still return the error if deletion/update failed
		return err
	}

	// batchPool.ReduceRemain(j.SpaceKey) // Removed - BatchPool handles completion internally
	// Log deletion success. ProgressString still works for getting current progress.
	log.Printf("%s Successfully deleted Dify document: %s", batchPool.ProgressString(j.SpaceKey), j.DocumentID)
	// Note: Since deletion doesn't involve batch monitoring, it completes immediately.
	// The BatchPool's total count for the space (set via SetTotal) should account for this.
	// If SetTotal counts only items needing monitoring, deletions shouldn't affect its count.
	// If SetTotal counts *all* operations (create/update/delete), then the BatchPool's
	// completed count won't reach the total unless deletions are also marked complete somehow.
	// Let's assume SetTotal counts only monitorable tasks (create/update/upload).
	return nil
}
