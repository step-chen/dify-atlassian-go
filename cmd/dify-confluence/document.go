package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// saveDocumentMapping saves the mapping between Confluence content and Dify document
func saveDocumentMapping(contentID, spaceKey, difyDocumentID, url, title, datasetID string, timestamp string) error {
	err := db.SaveMapping(contentID, spaceKey, difyDocumentID, url, title, datasetID, timestamp)
	if err != nil {
		log.Printf("Failed to save mapping for content %s: %v", title, err)
		return err
	}
	return nil
}

// processSpaceOperations processes all operations for a given space using worker queues
func processSpaceOperations(spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels) error {
	// Get space contents
	contents, err := confluenceClient.GetSpaceContentsList(spaceKey)
	if err != nil {
		return fmt.Errorf("error getting contents for space %s: %w", spaceKey, err)
	}

	// Initialize operations based on existing mappings
	if err := db.InitOperationByMapping(spaceKey, client.DatasetID(), contents); err != nil {
		return fmt.Errorf("error initializing operations for space %s: %w", spaceKey, err)
	}

	// Process each content operation
	for contentID, operation := range contents {
		if err := processContentOperation(contentID, operation, spaceKey, client, confluenceClient, jobChan); err != nil {
			return err
		}
	}

	return nil
}

// processContentOperation handles individual content operations based on type and action
func processContentOperation(contentID string, operation confluence.ContentOperation, spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels) error {
	switch operation.Type {
	case 0: // page
		switch operation.Action {
		case 0: // create
			// Create new page
			content, err := confluenceClient.GetContent(contentID)
			if err != nil {
				return fmt.Errorf("failed to get content %s: %w", contentID, err)
			}
			jobChan.Content <- jobContent{
				documentID: "",
				spaceKey:   spaceKey,
				content:    *content,
				client:     client,
				op:         operation,
			}

		case 1: // update
			// Update existing page
			content, err := confluenceClient.GetContent(contentID)
			if err != nil {
				return fmt.Errorf("failed to get content %s: %w", contentID, err)
			}
			jobChan.Content <- jobContent{
				documentID: operation.DifyID,
				spaceKey:   spaceKey,
				content:    *content,
				client:     client,
				op:         operation,
			}

		case 2: // delete
			// Delete document
			jobChan.Delete <- jobDelete{
				documentID: operation.DifyID,
				client:     client,
			}
		}

	case 1: // attachment
		switch operation.Action {
		case 0, 1: // create or update
			// Upload or update attachment
			attachment, err := confluenceClient.GetAttachment(contentID)
			if err != nil {
				return fmt.Errorf("failed to get attachment %s: %w", contentID, err)
			}
			jobChan.Attachment <- jobAttachment{
				documentID:       operation.DifyID,
				spaceKey:         spaceKey,
				attachment:       *attachment,
				client:           client,
				confluenceClient: confluenceClient,
				op:               operation,
			}

		case 2: // delete
			// Delete document
			jobChan.Delete <- jobDelete{
				documentID: operation.DifyID,
				client:     client,
			}
		}
	}
	return nil
}

func createDocument(j *jobContent) error {
	// Create context with 5 minute timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	var resp *dify.CreateDocumentResponse

	// Create new document
	docRequest := dify.CreateDocumentRequest{
		Name:              j.content.Title,
		Text:              j.content.Content,
		IndexingTechnique: "high_quality",
		DocForm:           "hierarchical_model",
	}

	resp, err := j.client.CreateDocumentByText(ctx, &docRequest)

	if err != nil {
		log.Printf("failed to create Dify document for space %s content %s: %v", j.spaceKey, j.content.Title, err)
		return err
	}

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.client, resp.Document.ID, j.content.URL, "page", j.spaceKey, j.content.Title, j.content.ID, j.content.PublishDate, ""); err != nil {
		if errDel := j.client.DeleteDocument(ctx, resp.Document.ID); errDel != nil {
			log.Printf("failed to delete Dify document %s: %v", resp.Document.ID, errDel)
		}
		return err
	}

	// Save mapping between Confluence and Dify
	if err := saveDocumentMapping(j.content.ID, j.spaceKey, resp.Document.ID, j.content.URL, j.content.Title, j.client.DatasetID(), j.content.PublishDate); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.spaceKey, j.content.ID, resp.Batch, j.op)

	log.Printf("successfully created Dify document for space %s content %s", j.spaceKey, j.content.Title)
	return nil
}

func updateDocument(j *jobContent) error {
	// Create context with 5 minute timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	var resp *dify.CreateDocumentResponse

	// Update document
	updateRequest := dify.UpdateDocumentRequest{
		Name: j.content.Title,
		Text: j.content.Content,
	}

	resp, err := j.client.UpdateDocumentByText(ctx, j.client.DatasetID(), j.documentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for space %s content %s: %v", j.spaceKey, j.content.Title, err)
		return err
	}

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.client, resp.Document.ID, j.content.URL, "page", j.spaceKey, j.content.Title, j.content.ID, j.content.PublishDate, ""); err != nil {
		return err
	}

	// Save mapping between Confluence and Dify
	if err := saveDocumentMapping(j.content.ID, j.spaceKey, resp.Document.ID, j.content.URL, j.content.Title, j.client.DatasetID(), j.content.PublishDate); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.spaceKey, j.content.ID, resp.Batch, j.op)

	log.Printf("successfully updated Dify document for space %s content %s", j.spaceKey, j.content.Title)
	return nil
}

func uploadDocumentByFile(j *jobAttachment) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var docResp *dify.CreateDocumentResponse

	// Download attachment using Confluence client
	showPath, filePath, err := j.confluenceClient.DownloadAttachment(j.attachment.Download, j.attachment.Title, j.attachment.MediaType)

	if err != nil {
		return fmt.Errorf("failed download for space %s attachment %s: %v", j.spaceKey, j.attachment.Title, err)
	}
	defer os.Remove(filePath)

	// Prepare document request data
	docMetadata := map[string]interface{}{
		"title":              j.attachment.Title,
		"creation_date":      j.attachment.LastModifiedDate,
		"last_modified_date": j.attachment.LastModifiedDate,
		"author":             j.attachment.Author,
		"document_type":      j.attachment.MediaType,
		"department/team":    "",
	}

	req := &dify.CreateDocumentByFileRequest{
		OriginalDocumentID: j.documentID,
		IndexingTechnique:  "high_quality",
		DocType:            "business_document",
		DocForm:            "hierarchical_model",
		DocMetadata:        docMetadata,
	}

	// Check media type and use appropriate upload method
	docResp, err = j.client.CreateDocumentByFile(ctx, filePath, req, showPath)

	if err != nil {
		return fmt.Errorf("failed to upload attachment for space %s attachment %s: %v", j.spaceKey, j.attachment.Title, err)
	}

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.client, docResp.Document.ID, j.attachment.Download, "attachment", j.spaceKey, j.attachment.Title, j.attachment.ID, j.attachment.LastModifiedDate, j.attachment.Download); err != nil {
		return err
	}

	// Save mapping between Confluence attachment and Dify document
	if err := saveDocumentMapping(j.attachment.ID, j.spaceKey, docResp.Document.ID, j.attachment.Download, j.attachment.Title, j.client.DatasetID(), j.attachment.LastModifiedDate); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.spaceKey, j.attachment.ID, docResp.Batch, j.op)

	if j.documentID == "" {
		log.Printf("successfully created Dify document for space %s attachment %s", j.spaceKey, j.attachment.Title)
	} else {
		log.Printf("successfully updated Dify document for space %s attachment %s", j.spaceKey, j.attachment.Title)
	}

	return nil
}

// updateDocumentMetadata updates document metadata with common fields
func updateDocumentMetadata(ctx context.Context, client *dify.Client, documentID, url, docType, spaceKey, title, id, timestamp, download string) error {
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
	if client.GetMetaID("id") != "" && id != "" {
		metadata = append(metadata, dify.DocumentMetadata{ID: client.GetMetaID("id"), Name: "id", Value: id})
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
	if err := client.UpdateDocumentMetadata(ctx, updateMetadataRequest); err != nil {
		log.Printf("failed to update metadata for Dify document %s: %v", documentID, err)
		return err
	}
	return nil
}

func deleteDocument(j *jobDelete) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Delete document
	err := j.client.DeleteDocument(ctx, j.documentID)
	if err != nil {
		log.Printf("Failed to delete Dify document %s: %v", j.documentID, err)
		return err
	}

	// Remove mapping
	err = db.DeleteMapping(j.documentID)
	if err != nil {
		log.Printf("Failed to delete mapping for document %s: %v", j.documentID, err)
		return err
	}

	log.Printf("Successfully deleted Dify document: %s", j.documentID)
	return nil
}
