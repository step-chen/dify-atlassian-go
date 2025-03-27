package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// processSpaceOperations processes all operations for a given space using worker queues
func processSpace(spaceKey string, client *dify.Client, confluenceClient *confluence.Client, jobChan *JobChannels, docMetas map[string]dify.DocumentInfo) error {
	// Get space contents
	contents, err := confluenceClient.GetSpaceContentsList(spaceKey)
	if err != nil {
		return fmt.Errorf("error getting contents for space %s: %w", spaceKey, err)
	}

	// Initialize operations based on existing mappings
	if err := initOperations(client, contents, docMetas); err != nil {
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

func initOperations(client *dify.Client, contents map[string]confluence.ContentOperation, docMetas map[string]dify.DocumentInfo) error {
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
				op.Action = 2 // Update if times differ
			} else {
				// Delete the operation since no action is needed
				delete(contents, contentID)
				continue
			}

			contents[contentID] = op
		}
	}

	batchPool.SetRemain(len(contents))
	batchPool.SetTotal(len(contents))
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
	// Create context with 2 minute timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	var resp *dify.CreateDocumentResponse

	// Create new document
	docRequest := dify.CreateDocumentRequest{
		Name:              j.Content.Title,
		Text:              j.Content.Content,
		IndexingTechnique: cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:           cfg.Dify.RagSetting.DocForm,
	}

	resp, err := j.Client.CreateDocumentByText(ctx, &docRequest)

	if err != nil {
		log.Printf("failed to create Dify document for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		return err
	}

	j.Op.DifyID = resp.Document.ID
	j.Op.DatasetID = j.Client.DatasetID()

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.Client, resp.Document.ID, j.Content.URL, "page", j.SpaceKey, j.Content.Title, j.Content.ID, j.Content.PublishDate, ""); err != nil {
		if errDel := j.Client.DeleteDocument(ctx, resp.Document.ID); errDel != nil {
			log.Printf("failed to delete Dify document %s: %v", resp.Document.ID, errDel)
		}
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.SpaceKey, j.Content.ID, j.Content.Title, resp.Batch, j.Op)

	return nil
}

func updateDocument(j *Job) error {
	// Create context with 2 minute timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	var resp *dify.CreateDocumentResponse

	// Update document
	updateRequest := dify.UpdateDocumentRequest{
		Name: j.Content.Title,
		Text: j.Content.Content,
	}

	resp, err := j.Client.UpdateDocumentByText(ctx, j.Client.DatasetID(), j.DocumentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for space %s content %s: %v", j.SpaceKey, j.Content.Title, err)
		return err
	}

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.Client, resp.Document.ID, j.Content.URL, "page", j.SpaceKey, j.Content.Title, j.Content.ID, j.Content.PublishDate, ""); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.SpaceKey, j.Content.ID, j.Content.Title, resp.Batch, j.Op)

	return nil
}

func uploadDocumentByFile(j *Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

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
	docResp, err = j.Client.CreateDocumentByFile(ctx, filePath, req, showPath)
	if err != nil {
		return fmt.Errorf("failed to upload attachment for space %s attachment %s: %v", j.SpaceKey, j.Attachment.Title, err)
	}

	j.Op.DifyID = docResp.Document.ID
	j.Op.DatasetID = j.Client.DatasetID()

	// Update document metadata
	if err := updateDocumentMetadata(ctx, j.Client, docResp.Document.ID, j.Attachment.Download, "attachment", j.SpaceKey, j.Attachment.Title, j.Attachment.ID, j.Attachment.LastModifiedDate, j.Attachment.Download); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	batchPool.Add(j.SpaceKey, j.Attachment.ID, j.Attachment.Title, docResp.Batch, j.Op)

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

func deleteDocument(j *Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Delete document
	err := j.Client.DeleteDocument(ctx, j.DocumentID)
	if err != nil {
		log.Printf("failed to delete Dify document %s: %v", j.DocumentID, err)
		return err
	}

	log.Printf("% *d/%d successfully deleted Dify document: %s", batchPool.GetTotalLen(), batchPool.GetCompleted(), batchPool.GetTotal(), j.DocumentID)
	return nil
}
