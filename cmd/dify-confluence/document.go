package main

import (
	"context" // Added context import
	"fmt"
	"log"
	"os"
	"strings" // Added strings import
	"time"

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
		if err := processContentOperation(contentID, operation, spaceKey, client, confluenceClient, jobChan); err != nil {
			return err
		}
	}

	return nil
}

func initOperations(client *dify.Client, contents map[string]confluence.ContentOperation) error {
	docMetas, err := client.FetchDocumentsList(0, 100)
	if err != nil {
		return fmt.Errorf("failed to list documents for dataset: %s: %v", client.DatasetID(), err)
	}

	for contentID, doc := range docMetas {
		if op, ok := contents[contentID]; !ok {
			// Add new operation for unmapped content
			contents[contentID] = confluence.ContentOperation{
				Action:    2, // Delete
				DifyID:    doc.DifyID,
				DatasetID: client.DatasetID(),
			}
		} else {
			// Update existing operation
			op.DifyID = doc.DifyID
			op.DatasetID = client.DatasetID()

			// Compare times using utility function
			equal, err := utils.CompareRFC3339Times(op.LastModifiedDate, doc.When)
			if err != nil {
				return fmt.Errorf("failed to compare times: %w", err)
			}

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

// processContentOperation handles individual content operations based on type and action
func processContentOperation(contentID string, operation confluence.ContentOperation, spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels) error {
	job := Job{
		Type:             JobType(operation.Type),
		DocumentID:       operation.DifyID,
		SpaceKey:         spaceKey,
		Client:           client,
		ConfluenceClient: confluenceClient,
		Op:               operation,
	}

	switch operation.Type {
	case 0: // page
		content, err := confluenceClient.GetContent(contentID)
		if err != nil {
			return fmt.Errorf("failed to get content %s: %w", contentID, err)
		}
		job.Content = content

	case 1: // attachment
		attachment, err := confluenceClient.GetAttachment(contentID)
		if err != nil {
			return fmt.Errorf("failed to get attachment %s: %w", contentID, err)
		}
		job.Attachment = attachment
	}

	jobChan.Jobs <- job
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

	// Update document metadata
	if err := updateDocumentMetadata(j.Client, resp.Document.ID, j.Content.URL, "page", j.SpaceKey, j.Content.Title, j.Content.ID, j.Content.PublishDate, ""); err != nil {
		// Pass Confluence ID (j.Content.ID) during cleanup deletion attempt
		if errDel := j.Client.DeleteDocument(resp.Document.ID, j.Content.ID); errDel != nil {
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
		Text: j.Content.Content,
	}

	resp, err := j.Client.UpdateDocumentByText(j.Client.DatasetID(), j.DocumentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		return err
	}

	j.Op.StartAt = time.Now()

	// Update document metadata
	if err := updateDocumentMetadata(j.Client, resp.Document.ID, j.Content.URL, "page", j.SpaceKey, j.Content.Title, j.Content.ID, j.Content.PublishDate, ""); err != nil {
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

func uploadDocumentByFile(j *Job) error {
	var docResp *dify.CreateDocumentResponse

	// Download attachment using Confluence client
	showPath, filePath, err := j.ConfluenceClient.DownloadAttachment(j.Attachment.Download, j.Attachment.Title, j.Attachment.MediaType)

	if err != nil {
		return fmt.Errorf("failed download for space %s attachment %s: %v", j.SpaceKey, j.Attachment.Title, err)
	}
	defer os.Remove(filePath)

	req := &dify.CreateDocumentByFileRequest{
		OriginalDocumentID: j.DocumentID,
		IndexingTechnique:  cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:            cfg.Dify.RagSetting.DocForm,
	}

	// Check media type and use appropriate upload method
	docResp, err = j.Client.CreateDocumentByFile(filePath, req, showPath)
	if err != nil {
		return fmt.Errorf("failed to upload attachment for space %s attachment %s: %v", j.SpaceKey, j.Attachment.Title, err)
	}

	j.Op.DifyID = docResp.Document.ID
	j.Op.DatasetID = j.Client.DatasetID()
	j.Op.StartAt = time.Now()

	// Update document metadata
	if err := updateDocumentMetadata(j.Client, docResp.Document.ID, j.Attachment.Download, "attachment", j.SpaceKey, j.Attachment.Title, j.Attachment.ID, j.Attachment.LastModifiedDate, j.Attachment.Download); err != nil {
		return err
	} // <--- Added missing closing brace

	// Add document to batch pool for indexing tracking
	// Add context.Background() as the first argument
	err = batchPool.Add(context.Background(), j.SpaceKey, j.Attachment.ID, j.Attachment.Title, docResp.Batch, j.Op)
	if err != nil {
		log.Printf("Error adding task to batch pool for space %s attachment %s: %v", j.SpaceKey, j.Attachment.Title, err)
	}

	return nil // Return nil even if adding to pool failed, as document upload succeeded
}

// updateDocumentMetadata updates document metadata with common fields
func updateDocumentMetadata(client *dify.Client, documentID, url, docType, spaceKey, title, id, timestamp, download string) error {
	metadata := []dify.DocumentMetadata{}
	if client.GetMetaID("url") != "" && url != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("url"), Name: "url", Value: url})
	}
	if client.GetMetaID("source_type") != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("source_type"), Name: "source_type", Value: "confluence"})
	}
	if client.GetMetaID("type") != "" && docType != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("type"), Name: "type", Value: docType})
	}
	if client.GetMetaID("space_key") != "" && spaceKey != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("space_key"), Name: "space_key", Value: spaceKey})
	}
	if client.GetMetaID("title") != "" && title != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("title"), Name: "title", Value: title})
	}
	// Handle 'id' metadata update logic
	if metaID := client.GetMetaID("id"); metaID != "" && id != "" {
		existingIDsStr := client.GetConfluenceIDsForDifyID(documentID)
		existingIDs := strings.Split(existingIDsStr, ",")
		idExists := false
		trimmedNewID := strings.TrimSpace(id)

		for _, existingID := range existingIDs {
			if strings.TrimSpace(existingID) == trimmedNewID {
				idExists = true
				break
			}
		}

		finalIDValue := existingIDsStr
		if !idExists {
			if existingIDsStr == "" {
				finalIDValue = trimmedNewID // First ID
			} else {
				finalIDValue = existingIDsStr + "," + trimmedNewID // Append new ID
			}
			// Update the mapping in the client instance as well
			client.SetMetaMapping(documentID, finalIDValue)
		}
		// Always append the metadata, using the potentially updated finalIDValue
		metadata = append(metadata, dify.DocumentMetadata{ID: metaID, Name: "id", Value: finalIDValue})
	}
	if client.GetMetaID("when") != "" && timestamp != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("when"), Name: "when", Value: timestamp})
	}
	if client.GetMetaID("download") != "" && download != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("download"), Name: "download", Value: download})
	}
	if len(metadata) == 0 {
		log.Printf("no metadata to update for Dify document %s", documentID)
		return nil
	}
	updateMetadataRequest := dify.UpdateDocumentMetadataRequest{
		OperationData: []dify.DocumentOperation{
			{
				DocumentID:   documentID,
				MetadataList: metadata,
			},
		},
	}

	if err := client.UpdateDocumentMetadata(updateMetadataRequest); err != nil {
		log.Printf("failed to update metadata for Dify document %s: %v", documentID, err)
		return err
	}
	return nil
}

func deleteDocument(j *Job) error {
	// Determine the Confluence ID based on the job type
	var confluenceID string
	if j.Type == JobTypeContent && j.Content != nil { // Changed JobTypePage to JobTypeContent
		confluenceID = j.Content.ID
	} else if j.Type == JobTypeAttachment && j.Attachment != nil {
		confluenceID = j.Attachment.ID
	} else {
		// Should not happen if job is constructed correctly
		log.Printf("Error: Could not determine Confluence ID for delete job with Dify ID %s", j.DocumentID)
		// Fallback: attempt deletion without specific confluence ID? Or return error?
		// Let's return an error as the DeleteDocument logic now relies on it.
		return fmt.Errorf("could not determine confluence ID for delete operation on Dify document %s", j.DocumentID)
	}

	// Delete document or update metadata
	err := j.Client.DeleteDocument(j.DocumentID, confluenceID)
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
