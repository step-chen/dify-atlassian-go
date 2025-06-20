package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/bitbucket"
	CFG "github.com/step-chen/dify-atlassian-go/internal/config/bitbucket"
	"github.com/step-chen/dify-atlassian-go/internal/content_parser"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// processSpaceOperations processes all operations for a given space using worker queues
func processKey(key string, client *dify.Client, bitbucketClient *bitbucket.Client, repoInfo *CFG.RepoInfo, jobChan *JobChannels) error {
	// Get space contents
	contents, err := bitbucketClient.GetContentsList(repoInfo)
	if err != nil {
		return fmt.Errorf("error getting contents for repos %s: %w", key, err)
	}

	// Initialize operations based on existing mappings
	if err := initOperations(client, contents); err != nil {
		return fmt.Errorf("error initializing operations for repos %s: %w", key, err)
	}

	batchPool.SetTotal(key, len(contents))

	parser, err := content_parser.NewParserByType(repoInfo.ParserType)
	if err != nil {
		return fmt.Errorf("failed to create parser for type %s: %w", repoInfo.ParserType, err)
	}

	// Process each content operation
	for fullPath, operation := range contents {
		if err := processOperation(fullPath, operation, key, repoInfo, &parser, client, bitbucketClient, jobChan); err != nil {
			log.Printf("error processing repos %s title %s media type %s: %v", key, fullPath, operation.MediaType, err)
		}
	}

	log.Printf("=========================================================")
	log.Printf("All operations for repos %s have been processed.", key)
	log.Printf("=========================================================")

	return nil
}

func initOperations(client *dify.Client, contents map[string]batchpool.Operation) error {
	bitbucketIDToDifyRecord, err := client.FetchDocuments(0, 100)
	if err != nil {
		return fmt.Errorf("failed to list documents for %s: %v", client.BaseURL(), err)
	}

	for contentID, record := range bitbucketIDToDifyRecord {
		if op, ok := contents[contentID]; !ok {
			contents[contentID] = batchpool.Operation{
				Action: batchpool.ActionDelete,
				DifyID: record.DifyID,
			}
		} else {
			op.DifyID = record.DifyID

			if record.Hash != op.Hash {
				op.Action = batchpool.ActionUpdate
			} else {
				delete(contents, contentID)
				continue
			}

			contents[contentID] = op
		}
	}

	return nil
}

// processOperation handles individual content operations based on type and action
func processOperation(fullPath string, operation batchpool.Operation, key string, repoInfo *CFG.RepoInfo, parser *content_parser.IParser, client *dify.Client, bitbucketClient *bitbucket.Client, jobChan *JobChannels) error {
	j := Job{
		Parser:          parser,
		Type:            operation.Type,
		DocumentID:      operation.DifyID,
		Key:             key,
		Client:          client,
		BitbucketClient: bitbucketClient,
		Op:              operation,
		RepoCFG:         repoInfo,
	}

	var content *bitbucket.Content
	var err error

	switch operation.Type {
	case batchpool.GitFile:
		content, err = bitbucketClient.GetContent(repoInfo, fullPath)
		if err != nil {
			return fmt.Errorf("failed to get content %s: %w", fullPath, err)
		}
		parsedContents, parseErr := (*j.Parser).ParseBytes(content.RAW)
		if parseErr != nil {
			return fmt.Errorf("failed to parse content %s: %w", fullPath, parseErr)
		}
		if len(parsedContents) == 0 {
			log.Printf("No parsed content for %s. Skipping.", fullPath)
			batchPool.MarkTaskComplete(key)
			return nil
		}
		// For now, we only take the first parsed content.
		// Future improvements might involve handling multiple parsed contents.
		content.Contents = parsedContents
	default:
		return fmt.Errorf("unsupported content type %d for content ID %s", operation.Type, fullPath)
	}

	if len(content.Contents) == 0 {
		if operation.DifyID != "" {
			if err := client.DeleteDocument(operation.DifyID, "bitbucket", fullPath); err != nil {
				log.Printf("failed to delete empty attachment %s: %v", fullPath, err)
			}
			log.Printf("Content for %s is empty. Dify document %s (if existed) deleted. Skipping further processing.", fullPath, operation.DifyID)
		}
		batchPool.MarkTaskComplete(key)
		return nil
	}

	j.Content = content
	difyID := client.GetDifyIDByHash(j.Content.Xxh3)
	if difyID != "" {
		params := dify.DocumentMetadataRecord{
			URL:        j.Content.URL,
			SourceType: "bitbucket", // Added SourceType
			Type:       j.Content.Type,
			Key:        j.Key,
			IDToAdd:    j.Content.ID, // Use the transient field to add this ID
			Hash:       j.Op.Hash,
			Xxh3:       j.Content.Xxh3, // Renamed from XXH3
			DifyID:     difyID,         // Added DifyID
		}

		if !j.Client.IsEqualDifyMeta(j.Content.ID, params) {
			// Update metadata using the new struct
			if err := j.Client.UpdateDocumentMetadata(difyID, "bitbucket", params); err != nil {
				log.Printf("failed to update document metadata for %s: %v", difyID, err)
			}
		}

		if j.DocumentID != difyID && j.DocumentID != "" {
			j.Client.DeleteDocument(j.DocumentID, "bitbucket", j.Content.ID)
		}
		batchPool.MarkTaskComplete(j.Key)
		return nil
	} else {
		j.Op.Action = batchpool.ActionCreate // Force create if no existing document with this hash
	}

	jobChan.Jobs <- j
	return nil
}

func createDocument(j *Job) error {
	content := combineContent(j.Content.Contents, j.RepoCFG.EOF+cfg.Dify.RagSetting.ProcessRule.Rules.Segmentation.Separator)
	keywords := make(map[int][]string, len(j.Content.Contents))
	for i, con := range j.Content.Contents {
		keywords[i] = con.Keywords
	}

	// Build document request using content metadata
	docRequest := dify.CreateDocumentRequest{
		Name:              j.Content.Title,
		Text:              content,
		IndexingTechnique: cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:           cfg.Dify.RagSetting.DocForm,
		ProcessRule:       cfg.Dify.RagSetting.ProcessRule,
	}

	// Create document via Dify client
	resp, err := j.Client.CreateDocumentByText(&docRequest, keywords)
	if err != nil {
		return err
	}

	// Store document ID in job context
	j.Op.DifyID = resp.Document.ID

	// Set hash mapping for future reference
	j.Client.SetHashMapping(j.Content.Xxh3, resp.Document.ID)

	// Update document metadata with source information
	params := dify.DocumentMetadataRecord{
		URL:        j.Content.URL,
		SourceType: "bitbucket",
		Type:       j.Content.Type,                  // Convert content type to string
		IDToAdd:    j.Content.ID,                    // Use the transient field to add this ID
		When:       time.Now().Format(time.RFC3339), // Use operation's modified timestamp
		Xxh3:       j.Content.Xxh3,
		Hash:       j.Op.Hash,
	}

	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, "bitbucket", params); err != nil {
		// Handle metadata update failure with cleanup
		if errDel := j.Client.DeleteDocument(resp.Document.ID, "bitbucket", j.Content.ID); errDel != nil {
			log.Printf("failed to delete/update Dify document %s after metadata update failure: %v", resp.Document.ID, errDel)
		}
		return err
	}

	// Track in batch pool for indexing progress
	if err := batchPool.Add(context.Background(), j.Key, j.Content.ID, j.Content.Title, resp.Batch, j.RepoCFG.EOF, j.Op); err != nil {
		log.Printf("error adding task to batch pool for %s content %s: %v", j.Key, j.Content.ID, err)
	}

	return nil
}

func updateDocument(j *Job) error {
	content := combineContent(j.Content.Contents, j.RepoCFG.EOF+cfg.Dify.RagSetting.ProcessRule.Rules.Segmentation.Separator)
	keywords := make(map[int][]string, len(j.Content.Contents))
	for i, con := range j.Content.Contents {
		keywords[i] = con.Keywords
	}

	// Build update request with latest content
	updateRequest := dify.UpdateDocumentRequest{
		Name:        j.Content.Title,
		Text:        content,
		ProcessRule: cfg.Dify.RagSetting.ProcessRule,
	}

	// Execute document update
	resp, err := j.Client.UpdateDocumentByText(j.DocumentID, &updateRequest, keywords)
	if err != nil {
		return err
	}

	// Update hash mapping for the content
	j.Client.SetHashMapping(j.Content.Xxh3, j.DocumentID)

	// Update document metadata with current information
	params := dify.DocumentMetadataRecord{
		URL:        j.Content.URL,
		SourceType: "bitbucket",
		Type:       j.Content.Type,
		IDToAdd:    j.Content.ID,
		When:       j.Op.LastModifiedDate,
		Xxh3:       j.Content.Xxh3,
		Hash:       j.Op.Hash,
	}

	if err := j.Client.UpdateDocumentMetadata(j.DocumentID, "bitbucket", params); err != nil {
		return err
	}

	// Track in batch pool for indexing progress
	if err := batchPool.Add(context.Background(), j.Key, j.Content.ID, j.Content.Title, resp.Batch, j.RepoCFG.EOF, j.Op); err != nil {
		log.Printf("Error adding task to batch pool for %s content %s: %v", j.Key, j.Content.ID, err)
	}

	return nil
}

func deleteDocument(j *Job) error {
	// Determine source ID based on job type
	var sourceID string
	if j.Type == batchpool.Page && j.Content != nil {
		sourceID = j.Content.ID
	} else if j.Type == batchpool.Attachment && j.Content != nil {
		sourceID = j.Content.ID
	} else {
		return fmt.Errorf("could not determine source ID for delete operation on Dify document %s", j.DocumentID)
	}

	// Execute document deletion with source tracking
	if err := j.Client.DeleteDocument(j.DocumentID, "bitbucket", sourceID); err != nil {
		return err
	}

	log.Printf("Successfully deleted Dify document: %s", j.DocumentID)
	return nil
}

func combineContent(contents []content_parser.ParsedContent, separator string) string {
	if len(contents) == 0 {
		return ""
	}

	var sb strings.Builder
	hasActualContent := false // To track if any non-empty content is added

	for _, pc := range contents {
		trimmedPcContent := strings.TrimSpace(pc.Content)
		if trimmedPcContent != "" {
			if hasActualContent { // Add separator only if there's preceding content
				sb.WriteString(separator)
			}
			sb.WriteString(trimmedPcContent)
			hasActualContent = true
		}
	}
	return sb.String()
}
