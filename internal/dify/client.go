package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings" // Import the strings package
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

type Client struct {
	baseURL     string
	apiKey      string
	datasetID   string
	httpClient  *http.Client
	config      *config.Config
	meta        map[string]MetaField              // map[metaName]MetaField
	metaMapping map[string]DocumentMetadataRecord // Metadata map[difyID]DocumentMetadataRecord
	hashMapping map[string]string                 // map[xxh3]difyID
	hashMutex   sync.RWMutex                      // Protects hashMapping
}

func (c *Client) DatasetID() string {
	return c.datasetID
}

func (c *Client) GetDocumentMetadataRecord(difyID string) (DocumentMetadataRecord, bool) {
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]DocumentMetadataRecord)
	}
	record, exists := c.metaMapping[difyID]
	return record, exists
}

func (c *Client) IsExistsForDifyID(difyID, confluenceID string) bool {
	record, exists := c.GetDocumentMetadataRecord(difyID)
	if !exists {
		return false
	}

	// Check if the confluenceID exists in the record's ConfluenceIDs
	ids := strings.Split(record.ConfluenceIDs, ",")
	for _, id := range ids {
		if strings.TrimSpace(id) == confluenceID {
			return true
		}
	}
	return false
}

func NewClient(baseURL, apiKey, datasetID string, cfg *config.Config) (*Client, error) {
	decryptedKey, err := config.Decrypt(apiKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt API key: %w", err)
	}

	return &Client{
		baseURL:     baseURL,
		apiKey:      decryptedKey,
		datasetID:   datasetID,
		httpClient:  &http.Client{},
		config:      cfg,
		metaMapping: make(map[string]DocumentMetadataRecord), // Initialize metaMapping
	}, nil
}

func (c *Client) GetIndexingStatus(spaceKey, batch string) (*IndexingStatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s/indexing-status", c.baseURL, c.datasetID, batch)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("unexpected status code: %d, datasetID: %s, url: %s", resp.StatusCode, c.datasetID, url)
		log.Printf("error response: %s", string(body))
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var statusResponse IndexingStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &statusResponse, nil
}

func (c *Client) CreateDocumentByText(req *CreateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	url := fmt.Sprintf("%s/datasets/%s/document/create-by-text", c.baseURL, c.datasetID)
	createDocRequest, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	createDocRequest.Header.Set("Content-Type", "application/json")
	createDocRequest.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(createDocRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("unexpected status code: %d, datasetID: %s, url: %s", resp.StatusCode, c.datasetID, url)
		log.Printf("error respone: %s", string(body))
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response CreateDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

func (c *Client) FetchDocumentsList(page, limit int) (map[string]DocumentMetadataRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	confluenceIDToDifyRecord := make(map[string]DocumentMetadataRecord)

	if page < 0 {
		page = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	for {
		url := fmt.Sprintf("%s/datasets/%s/documents?page=%d&limit=%d", c.baseURL, c.datasetID, page, limit)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("unexpected status code: %d, datasetID: %s, page: %d, limit: %d, url: %s",
				resp.StatusCode, c.datasetID, page, limit, url)
			log.Printf("error response: %s", string(body))
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var response DocumentListResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		for _, doc := range response.Data {
			var confluenceIDs, whenVal, xxh3Val string
			for _, meta := range doc.DocMetadata {
				if meta.Name == "id" {
					confluenceIDs = meta.Value
				} else if meta.Name == "when" {
					whenVal = meta.Value
				} else if meta.Name == "xxh3" {
					xxh3Val = meta.Value
				}
			}

			if confluenceIDs != "" {
				record := DocumentMetadataRecord{
					DifyID:        doc.ID,
					ConfluenceIDs: confluenceIDs,
					When:          whenVal,
					Xxh3:          xxh3Val,
				}
				c.SetHashMapping(xxh3Val, doc.ID)

				// Store the full record in the client's internal map (Dify ID -> Record)
				c.SetDocumentMetadataRecord(doc.ID, record)

				// Populate the return map (Confluence ID -> Record)
				ids := strings.Split(confluenceIDs, ",")
				for _, id := range ids {
					trimmedID := strings.TrimSpace(id)
					if trimmedID != "" {
						confluenceIDToDifyRecord[trimmedID] = record
					}
				}
			}
		}

		if !response.HasMore {
			break
		}

		page++
	}

	return confluenceIDToDifyRecord, nil
}

func (c *Client) UpdateDocumentByText(datasetID, documentID string, req *UpdateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s/update-by-text", c.baseURL, datasetID, documentID)

	var response CreateDocumentResponse
	maxRetries := 5
	baseDelay := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		updateRequest, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestBody))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		updateRequest.Header.Set("Content-Type", "application/json")
		updateRequest.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(updateRequest)
		if err != nil {
			if attempt == maxRetries-1 {
				return nil, fmt.Errorf("failed to send request after %d attempts: %w", maxRetries, err)
			}
			time.Sleep(time.Duration(attempt+1) * baseDelay)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
				return nil, fmt.Errorf("failed to decode response: %w", err)
			}
			return &response, nil
		}

		if resp.StatusCode < 500 && resp.StatusCode != 429 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("unexpected status code: %d, datasetID: %s, url: %s", resp.StatusCode, c.datasetID, url)
			log.Printf("error response: %s", string(body))
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		if attempt == maxRetries-1 {
			return nil, fmt.Errorf("unexpected status code after %d attempts: %d", maxRetries, resp.StatusCode)
		}

		time.Sleep(time.Duration(attempt+1) * baseDelay)
	}

	return &response, nil
}

func (c *Client) GetHashByDifyIDFromRecord(documentID string) string {
	record, exists := c.GetDocumentMetadataRecord(documentID)
	if !exists {
		return ""
	}
	return record.Xxh3
}

func (c *Client) GetDifyIDByHash(hash string) string {
	if c.hashMapping == nil {
		return ""
	}
	c.hashMutex.RLock()
	defer c.hashMutex.RUnlock()
	return c.hashMapping[hash]
}

func (c *Client) SetHashMapping(hash, difyID string) {
	c.hashMutex.Lock()
	defer c.hashMutex.Unlock()
	if c.hashMapping == nil {
		c.hashMapping = make(map[string]string)
	}
	c.hashMapping[hash] = difyID
}

func (c *Client) DeleteHashMapping(hash string) {
	c.hashMutex.Lock()
	defer c.hashMutex.Unlock()
	if c.hashMapping != nil {
		delete(c.hashMapping, hash)
	}
}

func (c *Client) DeleteMetaMapping(documentID string) {
	c.hashMutex.Lock()
	defer c.hashMutex.Unlock()
	if c.metaMapping != nil {
		delete(c.metaMapping, documentID)
	}
}

// DeleteDocument handles the removal of a Confluence ID association from a Dify document.
// If the removed ID is the last one associated with the Dify document, the document itself is deleted.
// Otherwise, the document's metadata is updated to remove the specified Confluence ID.
func (c *Client) DeleteDocument(documentID, confluenceIDToRemove string) error {
	record, exists := c.GetDocumentMetadataRecord(documentID)
	if !exists {
		// If the record doesn't exist locally, it might still exist in Dify.
		// Log a warning and attempt direct deletion, assuming this might be the only ID.
		log.Printf("Warning: No local metadata record found for Dify document %s during deletion attempt for Confluence ID %s. Attempting direct deletion.", documentID, confluenceIDToRemove)
		return c.performDeleteRequest(documentID)
	}

	// Filter out the confluenceIDToRemove
	existingIDs := strings.Split(record.ConfluenceIDs, ",")
	remainingIDs := make([]string, 0, len(existingIDs))
	found := false
	for _, id := range existingIDs {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == confluenceIDToRemove {
			found = true
		} else if trimmedID != "" {
			remainingIDs = append(remainingIDs, trimmedID)
		}
	}

	// If the ID to remove wasn't found in the record, log a warning but proceed.
	// It's possible the local cache is stale. The update/delete operation will handle it.
	if !found {
		log.Printf("Warning: Confluence ID %s not found in local metadata record for Dify document %s (%s). Proceeding with operation.", confluenceIDToRemove, documentID, record.ConfluenceIDs)
	}

	// Decide whether to update metadata or delete the document
	if len(remainingIDs) > 0 {
		// Update metadata if other IDs remain
		log.Printf("Dify document %s has other associated Confluence IDs (%s). Updating metadata.", documentID, strings.Join(remainingIDs, ","))
		return c.updateDocumentConfluenceIDs(documentID, record, remainingIDs)
	} else {
		// Delete the document if this was the last ID
		log.Printf("Confluence ID %s is the last known association for Dify document %s. Proceeding with deletion.", confluenceIDToRemove, documentID)
		return c.performDeleteRequest(documentID)
	}
}

// updateDocumentConfluenceIDs updates the 'id' metadata field for a Dify document.
func (c *Client) updateDocumentConfluenceIDs(documentID string, originalRecord DocumentMetadataRecord, remainingIDs []string) error {
	newIDsStr := strings.Join(remainingIDs, ",")

	metaIDFieldID := c.GetMetaID("id")
	if metaIDFieldID == "" {
		// This is critical, as we cannot update the IDs without the field ID.
		return fmt.Errorf("critical error: metadata field 'id' not found in client config for dataset %s. Cannot update document %s", c.datasetID, documentID)
	}

	// Prepare the metadata update request specifically for the 'id' field
	metadataToUpdate := []DocumentMetadata{
		{ID: metaIDFieldID, Name: "id", Value: newIDsStr},
	}

	// Add other metadata fields if they exist in the original record and are configured
	addMeta := func(fieldName, value string) {
		fieldID := c.GetMetaID(fieldName)
		if fieldID != "" && value != "" {
			// Check if the field is already in the list to avoid duplicates (though 'id' is the primary focus)
			alreadyExists := false
			for _, meta := range metadataToUpdate {
				if meta.Name == fieldName {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				metadataToUpdate = append(metadataToUpdate, DocumentMetadata{ID: fieldID, Name: fieldName, Value: value})
			}
		} else if fieldID == "" && value != "" {
			log.Printf("Warning: Metadata field '%s' has value in record but is not configured in Dify meta for dataset %s. Skipping update for this field.", fieldName, c.datasetID)
		}
	}

	// Include other relevant metadata fields from the original record
	addMeta("url", originalRecord.URL)
	addMeta("source_type", "confluence") // Assuming it's always confluence here
	addMeta("type", originalRecord.Type)
	addMeta("space_key", originalRecord.SpaceKey)
	addMeta("title", originalRecord.Title)
	addMeta("when", originalRecord.When)
	addMeta("xxh3", originalRecord.Xxh3)

	updateReq := UpdateDocumentMetadataRequest{
		OperationData: []DocumentOperation{
			{
				DocumentID:   documentID,
				MetadataList: metadataToUpdate,
			},
		},
	}

	// Perform the update API call
	err := c.updateDocumentMetadataByRequest(updateReq)
	if err != nil {
		log.Printf("Failed to update metadata for Dify document %s after removing Confluence ID: %v", documentID, err)
		// No need to restore the local record here, as the API call failed. The local state remains unchanged until success.
		return fmt.Errorf("failed to update metadata for document %s: %w", documentID, err)
	}

	// Update local cache only on successful API call
	updatedRecord := originalRecord
	updatedRecord.ConfluenceIDs = newIDsStr
	c.SetDocumentMetadataRecord(documentID, updatedRecord)

	return nil
}

// performDeleteRequest sends the actual DELETE request to the Dify API.
func (c *Client) performDeleteRequest(documentID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s", c.baseURL, c.datasetID, documentID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request for %s: %w", documentID, err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send delete request for %s: %w", documentID, err)
	}
	defer resp.Body.Close()

	// Check for successful status codes (200 OK or 204 No Content)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Document deletion failed for %s. Status: %d, datasetID: %s, url: %s", documentID, resp.StatusCode, c.datasetID, url)
		log.Printf("error response: %s", string(body))
		// Handle 404 Not Found specifically - maybe the document was already deleted
		if resp.StatusCode == http.StatusNotFound {
			log.Printf("Document %s not found in Dify. It might have been deleted already.", documentID)
			// Clean up local cache even if deletion failed because it wasn't found
			c.cleanupLocalCache(documentID)
			return nil // Treat as success if not found
		}
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Clean up local cache on successful deletion
	c.cleanupLocalCache(documentID)
	log.Printf("Successfully deleted Dify document %s.", documentID)
	return nil
}

// cleanupLocalCache removes the document's entries from internal mappings.
func (c *Client) cleanupLocalCache(documentID string) {
	// Get hash before deleting meta mapping
	hash := c.GetHashByDifyIDFromRecord(documentID)
	c.DeleteMetaMapping(documentID)
	if hash != "" {
		c.DeleteHashMapping(hash)
	}
}
