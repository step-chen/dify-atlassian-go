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
	apiKey      string // Store the decrypted key directly
	datasetID   string // Store the specific dataset ID string
	httpClient  *http.Client
	config      config.DifyCfgProvider            // Use the interface
	meta        map[string]MetaField              // map[metaName]MetaField
	metaMapping map[string]DocumentMetadataRecord // Metadata map[difyID]DocumentMetadataRecord
	hashMapping map[string]string                 // map[xxh3]difyID
	hashMutex   sync.RWMutex                      // Protects hashMapping
}

func (c *Client) DatasetID() string {
	return c.datasetID
}

// GetConfig returns the configuration provider associated with the client.
func (c *Client) GetConfig() config.DifyCfgProvider {
	return c.config
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

// NewClient now accepts the DifyClientConfigProvider interface, the decrypted API key, and the specific dataset ID.
func NewClient(baseURL, decryptedAPIKey string, datasetID string, cfgProvider config.DifyCfgProvider) (*Client, error) {
	if decryptedAPIKey == "" {
		return nil, fmt.Errorf("decrypted API key cannot be empty")
	}
	if datasetID == "" {
		return nil, fmt.Errorf("dataset ID cannot be empty")
	}
	if cfgProvider == nil {
		return nil, fmt.Errorf("config provider cannot be nil")
	}

	return &Client{
		baseURL:     baseURL,
		apiKey:      decryptedAPIKey, // Store the provided decrypted key
		datasetID:   datasetID,       // Store the provided dataset ID
		httpClient:  &http.Client{},
		config:      cfgProvider,                             // Store the interface
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
	// Use the config provider interface to get the default process rule
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config) // Pass the stored interface
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

func (c *Client) UpdateDocumentByText(documentID string, req *UpdateDocumentRequest) (*CreateDocumentResponse, error) {
	// Use the config provider interface to get the default process rule if needed
	if req.ProcessRule.Mode == "" {
		// Only set default if ProcessRule itself is provided but Mode is empty
		// If req.ProcessRule is entirely nil/zero, the API might use its own defaults.
		// However, the struct definition includes ProcessRule directly, not as a pointer,
		// so we check the Mode. If Mode is empty, we apply defaults.
		req.ProcessRule = DefaultProcessRule(c.config) // Pass the stored interface
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s/update-by-text", c.baseURL, c.datasetID, documentID)

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

// AddChunks adds new segments (chunks) to an existing document in the specified dataset.
func (c *Client) AddChunks(documentID, content string) error {
	if documentID == "" && content == "" {
		return fmt.Errorf("error document ID or name is empty")
	}

	req := AddChunksRequest{
		Segments: []Segment{
			{
				Content: content,
			},
		},
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal AddChunks request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Adjust timeout as needed
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s/segments", c.baseURL, c.datasetID, documentID)

	addChunksReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create AddChunks request: %w", err)
	}

	addChunksReq.Header.Set("Content-Type", "application/json")
	addChunksReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(addChunksReq)
	if err != nil {
		return fmt.Errorf("failed to send AddChunks request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add title chunks failed: unexpected status code %d; msg: %s", resp.StatusCode, string(body))
	}

	return nil
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
		return c.updateDocumentConfluenceIDs(documentID, record, remainingIDs)
	} else {
		// Delete the document if this was the last ID
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

	return nil
}

// GetAllDocumentsMetadata fetches all document metadata for the dataset, specifically parsing fields
// relevant for local file synchronization (doc_id, original_path, last_modified, content_hash).
// It returns a map where the key is the doc_id (our internal identifier) and the value is LocalFileMetadata.
func (c *Client) GetAllDocumentsMetadata() (map[string]LocalFileMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Increased timeout for potentially large datasets
	defer cancel()

	localMetaMap := make(map[string]LocalFileMetadata)
	page := 1    // Dify API pages start from 1
	limit := 100 // Max limit

	log.Printf("Fetching all document metadata for dataset %s...", c.datasetID)

	for {
		url := fmt.Sprintf("%s/datasets/%s/documents?page=%d&limit=%d&metadata_only=true", c.baseURL, c.datasetID, page, limit) // Use metadata_only=true

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request for page %d: %w", page, err)
		}

		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Implement retry logic for transient network errors if necessary
			return nil, fmt.Errorf("failed to send request for page %d: %w", page, err)
		}

		// Check status code before attempting to read/close body
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close() // Close body even on error
			log.Printf("unexpected status code %d fetching metadata page %d for dataset %s: %s", resp.StatusCode, page, c.datasetID, string(bodyBytes))
			return nil, fmt.Errorf("unexpected status code %d fetching metadata page %d", resp.StatusCode, page)
		}

		// Decode response
		var response DocumentListResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			resp.Body.Close() // Close body after decoding attempt
			return nil, fmt.Errorf("failed to decode response for page %d: %w", page, err)
		}
		resp.Body.Close() // Close body successfully

		if len(response.Data) == 0 && !response.HasMore {
			// Sometimes HasMore might be true even if Data is empty on the last page? Double check.
			// If Data is empty, we assume we are done.
			break
		}

		// Process documents in the current page
		for _, doc := range response.Data {
			meta := LocalFileMetadata{
				DifyDocumentID: doc.ID,
			}
			var docIDValue, originalPathValue, lastModifiedValue, contentHashValue string

			// Extract specific metadata fields
			for _, apiMeta := range doc.DocMetadata {
				switch apiMeta.Name {
				case "doc_id":
					docIDValue = apiMeta.Value
				case "original_path":
					originalPathValue = apiMeta.Value
				case "last_modified": // Assuming stored as RFC3339 string
					lastModifiedValue = apiMeta.Value
				case "content_hash": // Assuming this is the xxh3 hash
					contentHashValue = apiMeta.Value
					// We can ignore other fields like 'id' (confluence id), 'when', 'xxh3' if not needed for local sync logic
				}
			}

			// Validate required fields and parse time
			if docIDValue == "" {
				log.Printf("Warning: Skipping document %s in dataset %s due to missing 'doc_id' metadata.", doc.ID, c.datasetID)
				continue
			}
			if originalPathValue == "" {
				log.Printf("Warning: Skipping document %s (doc_id: %s) in dataset %s due to missing 'original_path' metadata.", doc.ID, docIDValue, c.datasetID)
				continue
			}

			parsedTime, err := time.Parse(time.RFC3339, lastModifiedValue)
			if err != nil {
				log.Printf("Warning: Skipping document %s (doc_id: %s) in dataset %s due to invalid 'last_modified' format ('%s'): %v", doc.ID, docIDValue, c.datasetID, lastModifiedValue, err)
				continue // Skip if time cannot be parsed
			}

			// Populate the struct
			meta.DocID = docIDValue
			meta.OriginalPath = originalPathValue
			meta.LastModified = parsedTime
			meta.ContentHash = contentHashValue // Store the hash

			// Store in the map using doc_id as the key
			localMetaMap[meta.DocID] = meta
		}

		// Break if no more pages
		if !response.HasMore {
			break
		}

		page++
		// Optional: Add a small delay between pages to avoid rate limiting
		// time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Fetched %d document metadata records for dataset %s.", len(localMetaMap), c.datasetID)
	return localMetaMap, nil
}

// BuildLocalFileMetadataPayload constructs the metadata payload for Dify API calls.
// It uses the client's internal meta map to get the correct Dify field IDs.
func (c *Client) BuildLocalFileMetadataPayload(docID, originalPath string, lastModified time.Time, contentHash string) ([]DocumentMetadata, error) {
	payload := []DocumentMetadata{}
	lastModifiedStr := lastModified.UTC().Format(time.RFC3339) // Format time for Dify

	// Helper to add metadata if the field exists in c.meta and value is not empty
	addMeta := func(fieldName, value string) error {
		if value == "" {
			return nil // Don't add empty values
		}
		metaField, exists := c.meta[fieldName]
		if !exists {
			// Log a warning but don't fail if a field isn't configured in Dify
			log.Printf("Warning: Metadata field '%s' not found in Dify configuration for dataset %s. Skipping.", fieldName, c.datasetID)
			return nil
			// Alternatively, return an error if these fields are critical:
			// return fmt.Errorf("metadata field '%s' not configured in Dify for dataset %s", fieldName, c.datasetID)
		}
		payload = append(payload, DocumentMetadata{
			ID:    metaField.ID, // Use the ID from c.meta
			Name:  fieldName,    // Include name for clarity, though API might only need ID
			Value: value,
		})
		return nil
	}

	// Add required fields
	if err := addMeta("doc_id", docID); err != nil {
		return nil, err
	}
	if err := addMeta("original_path", originalPath); err != nil {
		return nil, err
	}
	if err := addMeta("last_modified", lastModifiedStr); err != nil {
		return nil, err
	}
	if err := addMeta("content_hash", contentHash); err != nil {
		return nil, err
	}
	// Add source_type automatically
	if err := addMeta("source_type", "local_folder"); err != nil {
		return nil, err
	}

	return payload, nil
}

// UpdateMetadataForDocument sends a request to update the metadata for a specific document.
func (c *Client) UpdateMetadataForDocument(difyDocumentID string, metadataPayload []DocumentMetadata) error {
	if difyDocumentID == "" {
		return fmt.Errorf("Dify document ID cannot be empty for metadata update")
	}
	if len(metadataPayload) == 0 {
		log.Printf("No metadata payload provided for document %s. Skipping metadata update API call.", difyDocumentID)
		return nil // Nothing to update
	}

	updateReq := UpdateDocumentMetadataRequest{
		OperationData: []DocumentOperation{
			{
				DocumentID:   difyDocumentID,
				MetadataList: metadataPayload,
			},
		},
	}

	// Use the internal method that handles the API call
	err := c.updateDocumentMetadataByRequest(updateReq)
	if err != nil {
		log.Printf("Failed to update metadata via API for Dify document %s: %v", difyDocumentID, err)
		return fmt.Errorf("failed to update Dify metadata via API for document %s: %w", difyDocumentID, err)
	}

	log.Printf("Successfully updated metadata via API for Dify document %s", difyDocumentID)
	return nil
}

// UpdateLocalFileMetadataCache updates the client's internal cache (metaMapping and hashMapping)
// with the provided LocalFileMetadata.
func (c *Client) UpdateLocalFileMetadataCache(meta LocalFileMetadata) error {
	if meta.DifyDocumentID == "" {
		return fmt.Errorf("cannot update cache without DifyDocumentID")
	}
	if meta.DocID == "" {
		return fmt.Errorf("cannot update cache without DocID")
	}

	// --- Hash Mapping Update Logic ---
	oldHash := ""
	// Lock for reading existing metaMapping
	c.hashMutex.RLock()
	if c.metaMapping != nil {
		if existingRecord, exists := c.metaMapping[meta.DifyDocumentID]; exists {
			// Note: metaMapping stores DocumentMetadataRecord, not LocalFileMetadata.
			// We need to adapt how we get the old hash. Let's assume 'xxh3' field exists.
			// This highlights a potential inconsistency between local file meta and confluence meta storage.
			// For now, let's assume we store LocalFileMetadata directly or adapt GetDocumentMetadataRecord.
			// **Correction:** Let's stick to DocumentMetadataRecord in metaMapping for now and extract xxh3.
			oldHash = existingRecord.Xxh3 // Assuming xxh3 field holds the content hash
		}
	}
	c.hashMutex.RUnlock()

	// --- Update metaMapping ---
	// We need to store DocumentMetadataRecord, so we convert/create one.
	// This assumes we want to keep the metaMapping structure consistent.
	// Alternatively, we could have a separate cache for LocalFileMetadata.
	// Let's create/update a DocumentMetadataRecord.
	recordToStore := DocumentMetadataRecord{
		DifyID: meta.DifyDocumentID,
		// Map LocalFileMetadata fields to DocumentMetadataRecord fields
		// We might not have direct equivalents for all fields (e.g., ConfluenceIDs).
		// Store the essential local file info in available fields or specific local file fields.
		// Using 'id' for docID, 'url' for originalPath, 'when' for lastModified, 'xxh3' for contentHash
		ConfluenceIDs: meta.DocID, // Using ConfluenceIDs field to store our internal docID
		URL:           meta.OriginalPath,
		When:          meta.LastModified.UTC().Format(time.RFC3339),
		Xxh3:          meta.ContentHash,
		SourceType:    "local_folder", // Indicate the source
		// Other fields like Title, SpaceKey, Type might be empty or set differently for local files
	}
	c.SetDocumentMetadataRecord(meta.DifyDocumentID, recordToStore) // Use the existing method to store

	// --- Update hashMapping ---
	newHash := meta.ContentHash
	if newHash != "" && newHash != oldHash {
		// Remove old mapping first if old hash existed
		if oldHash != "" {
			c.DeleteHashMapping(oldHash)
		}
		// Add new mapping
		c.SetHashMapping(newHash, meta.DifyDocumentID)
		log.Printf("Updated hash mapping: %s -> %s (Old: %s)", newHash, meta.DifyDocumentID, oldHash)
	} else if newHash == "" && oldHash != "" {
		// If new hash is empty but old one existed, remove the old mapping
		c.DeleteHashMapping(oldHash)
		log.Printf("Removed hash mapping for old hash: %s (DifyID: %s)", oldHash, meta.DifyDocumentID)
	}

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
