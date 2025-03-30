package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strings" // Import the strings package
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

// Client handles communication with the Dify API
type Client struct {
	baseURL     string                                // API endpoint base URL
	apiKey      string                                // Decrypted API key for authentication
	datasetID   string                                // Target dataset ID for operations
	httpClient  *http.Client                          // HTTP client with default timeout
	config      *config.Config                        // Application configuration
	meta        map[string]MetaField                  // map[metaName]MetaField
	metaMapping map[string]DifyDocumentMetadataRecord // Metadata map[difyID]DifyDocumentMetadataRecord
}

// DatasetID returns the configured dataset ID for this client
func (c *Client) DatasetID() string {
	return c.datasetID
}

// GetDocumentMetadataRecord retrieves the stored metadata record for a given Dify document ID.
func (c *Client) GetDocumentMetadataRecord(difyID string) (DifyDocumentMetadataRecord, bool) {
	// Ensure metaMapping is initialized before accessing
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]DifyDocumentMetadataRecord) // Or log an error if it should always be initialized
	}
	record, exists := c.metaMapping[difyID]
	return record, exists
}

// GetConfluenceIDsForDifyID retrieves the stored comma-separated Confluence IDs from the metadata record for a given Dify document ID.
func (c *Client) GetConfluenceIDsForDifyID(difyID string) string {
	record, exists := c.GetDocumentMetadataRecord(difyID)
	if !exists {
		return ""
	}
	return record.ConfluenceIDs // Returns "" if key doesn't exist or record has no IDs
}

// SetDocumentMetadataRecord updates the internal metadata record for a given Dify ID.
func (c *Client) SetDocumentMetadataRecord(difyID string, record DifyDocumentMetadataRecord) {
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]DifyDocumentMetadataRecord)
	}
	// Ensure the DifyID within the record matches the key
	record.DifyID = difyID
	c.metaMapping[difyID] = record
}

// NewClient initializes a new Dify API client
// baseURL: API endpoint URL
// apiKey: Encrypted API key
// datasetID: Target dataset ID
// cfg: Application configuration
// Returns initialized client or error
func NewClient(baseURL, apiKey, datasetID string, cfg *config.Config) (*Client, error) {
	// Decrypt API Key
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
		metaMapping: make(map[string]DifyDocumentMetadataRecord), // Initialize metaMapping
	}, nil
}

// GetIndexingStatus checks document indexing progress
// ctx: Context for request cancellation
// spaceKey: Target space identifier
// batch: Batch ID to check status for
// Returns indexing status details or error
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
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var statusResponse IndexingStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &statusResponse, nil
}

// CreateDocumentByText creates document from text content
// ctx: Context for request cancellation
// req: Document creation request details
// Returns creation response or error
func (c *Client) CreateDocumentByText(req *CreateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}
	// Create context with 2 minute timeout to prevent hanging
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
		log.Printf("unexpected status code: %d, datasetID: %s, url: %s", resp.StatusCode, c.datasetID, url)
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response CreateDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// CreateDocumentByFile creates document from file upload
// ctx: Context for request cancellation
// filePath: Path to source file
// req: Document creation request details
// title: Document title
// Returns creation response or error
func (c *Client) CreateDocumentByFile(filePath string, req *CreateDocumentByFileRequest, title string) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w, path: %s", err, filePath)
	}
	defer file.Close()

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	dataJSON, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data: %w", err)
	}

	jsonPart, err := writer.CreateFormField("data")
	if err != nil {
		return nil, fmt.Errorf("failed to create data field: %w", err)
	}

	if _, err = jsonPart.Write(dataJSON); err != nil {
		return nil, fmt.Errorf("failed to write data field: %w", err)
	}

	filePart, err := writer.CreateFormFile("file", title)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := io.Copy(filePart, file); err != nil {
		return nil, fmt.Errorf("failed to copy file content: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/document/create-by-file", c.baseURL, c.datasetID)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, &requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("unexpected status code: %d, datasetID: %s, title: %s, url: %s", resp.StatusCode, c.datasetID, title, url)
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response CreateDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// FetchDocumentsList retrieves paginated document list and populates the internal metaMapping.
// It returns a map where the key is the Confluence Content ID and the value is the DifyDocumentMetadataRecord.
// page: Starting page number (0-based)
// limit: Number of items per page (max 100)
// Returns map[confluenceID]DifyDocumentMetadataRecord or error
func (c *Client) FetchDocumentsList(page, limit int) (map[string]DifyDocumentMetadataRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// This map will be returned, mapping Confluence ID -> Dify Metadata Record
	confluenceIDToDifyRecord := make(map[string]DifyDocumentMetadataRecord)

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
			log.Printf("unexpected status code: %d, datasetID: %s, page: %d, limit: %d, url: %s",
				resp.StatusCode, c.datasetID, page, limit, url)
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var response DocumentListResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		for _, doc := range response.Data {
			var confluenceIDs, whenValue string
			// Extract 'id' (Confluence IDs) and 'when' from metadata
			for _, meta := range doc.DocMetadata {
				// Assuming the metadata field for Confluence IDs is named 'id'
				if meta.Name == "id" {
					confluenceIDs = meta.Value
				} else if meta.Name == "when" {
					whenValue = meta.Value
				}
				// TODO: Potentially extract other fields like url, type, space_key, title, download if they are stored in Dify metadata
			}

			if confluenceIDs != "" {
				// Create the metadata record for this Dify document
				record := DifyDocumentMetadataRecord{
					DifyID:        doc.ID,
					ConfluenceIDs: confluenceIDs,
					When:          whenValue,
					// Initialize other fields if available from metadata or leave empty
				}

				// Store the full record in the client's internal map (Dify ID -> Record)
				c.SetDocumentMetadataRecord(doc.ID, record)

				// Populate the return map (Confluence ID -> Record)
				ids := strings.Split(confluenceIDs, ",")
				for _, id := range ids {
					trimmedID := strings.TrimSpace(id)
					if trimmedID != "" {
						// If multiple Confluence IDs map to the same Dify ID,
						// this will overwrite previous entries for the same Confluence ID.
						// This assumes a Confluence ID maps to only one Dify document.
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

// UpdateDocumentByText modifies existing document content
// ctx: Context for request cancellation
// datasetID: Target dataset ID
// documentID: Document ID to update
// req: Update request details
// Returns update response or error
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

		// Only retry on server errors (5xx) or rate limiting (429)
		if resp.StatusCode < 500 && resp.StatusCode != 429 {
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		if attempt == maxRetries-1 {
			return nil, fmt.Errorf("unexpected status code after %d attempts: %d", maxRetries, resp.StatusCode)
		}

		time.Sleep(time.Duration(attempt+1) * baseDelay)
	}

	return &response, nil
}

// DeleteDocument handles removing a Confluence ID association from a Dify document.
// If the document is associated with multiple Confluence IDs, it only updates the metadata.
// If it's the last associated Confluence ID, it deletes the Dify document entirely.
// documentID: The Dify document ID.
// confluenceIDToRemove: The specific Confluence ID to disassociate.
// Returns error if the operation fails.
func (c *Client) DeleteDocument(documentID, confluenceIDToRemove string) error {
	record, exists := c.GetDocumentMetadataRecord(documentID)
	if !exists {
		// If there's no mapping, maybe the document was already deleted or never mapped?
		// Attempt deletion anyway, or log a warning. Let's try deleting.
		log.Printf("Warning: No metadata record found for Dify document %s during deletion attempt for Confluence ID %s. Attempting direct deletion.", documentID, confluenceIDToRemove)
		// Fall through to actual deletion logic below.
	}

	existingIDs := strings.Split(record.ConfluenceIDs, ",")
	remainingIDs := []string{}
	found := false

	for _, id := range existingIDs {
		trimmedID := strings.TrimSpace(id)
		if trimmedID == confluenceIDToRemove {
			found = true // Mark that we found the ID to remove
		} else if trimmedID != "" {
			remainingIDs = append(remainingIDs, trimmedID) // Keep other valid IDs
		}
	}

	// If the ID to remove wasn't even in the list, log warning and maybe still delete?
	if !found && record.ConfluenceIDs != "" { // Check if ConfluenceIDs was actually populated
		log.Printf("Warning: Confluence ID %s not found in metadata record for Dify document %s (%s). Proceeding with potential deletion.", confluenceIDToRemove, documentID, record.ConfluenceIDs)
		// Decide if we should still delete. Let's assume yes for now.
	}

	// If there are other IDs remaining after removal
	if len(remainingIDs) > 0 {
		log.Printf("Dify document %s has other associated Confluence IDs. Updating metadata instead of deleting.", documentID)
		newIDsStr := strings.Join(remainingIDs, ",")

		// Update the record in the client's internal map first
		updatedRecord := record // Make a copy to modify
		updatedRecord.ConfluenceIDs = newIDsStr
		c.SetDocumentMetadataRecord(documentID, updatedRecord) // Update internal map

		// Prepare metadata update request for Dify API
		metaIDFieldID := c.GetMetaID("id") // Get the Dify Field ID for the 'id' metadata
		if metaIDFieldID == "" {
			// This should not happen if InitMetadata ran correctly
			return fmt.Errorf("critical error: metadata field 'id' not found in client config for dataset %s", c.datasetID)
		}

		// Prepare the list of metadata fields to update, mirroring updateDocumentMetadata logic
		metadataToUpdate := []DocumentMetadata{}

		// Helper function to add metadata if valid
		addMeta := func(fieldName, value string) {
			fieldID := c.GetMetaID(fieldName)
			if fieldID != "" && value != "" {
				metadataToUpdate = append(metadataToUpdate, DocumentMetadata{ID: fieldID, Name: fieldName, Value: value})
			} else if fieldID == "" {
				// Log if a field from the record exists but isn't configured in Dify meta
				// This helps diagnose potential configuration mismatches.
				// Only log if the value is not empty, otherwise it's just an unused field.
				if value != "" {
					log.Printf("Warning: Metadata field '%s' has value in record but is not configured in Dify meta for dataset %s. Skipping update for this field.", fieldName, c.datasetID)
				}
			}
		}

		// Add 'id' field (always attempt if configured)
		if metaIDFieldID != "" {
			metadataToUpdate = append(metadataToUpdate, DocumentMetadata{ID: metaIDFieldID, Name: "id", Value: newIDsStr})
		} else {
			// This case is handled by the error check above, but included for completeness
			log.Printf("Critical Error: Metadata field 'id' not configured. Cannot update remaining IDs.")
			// The error is already returned above.
		}

		// Add other fields based on the existing record
		addMeta("url", record.URL)
		addMeta("source_type", "confluence") // Always try to set source_type if configured
		addMeta("type", record.Type)
		addMeta("space_key", record.SpaceKey)
		addMeta("title", record.Title)
		addMeta("when", record.When)
		addMeta("download", record.Download)

		// Only proceed if there's actually metadata to update (at least 'id' should be there if configured)
		if len(metadataToUpdate) == 0 {
			log.Printf("Warning: No configured metadata fields to update for Dify document %s after removing Confluence ID %s. This might indicate a configuration issue.", documentID, confluenceIDToRemove)
			// Decide if this is an error or just a warning. Let's treat as warning for now.
			// We still updated the internal map, so return nil.
			return nil
		}

		updateReq := UpdateDocumentMetadataRequest{
			OperationData: []DocumentOperation{
				{
					DocumentID:   documentID,
					MetadataList: metadataToUpdate, // Use the constructed list
				},
			},
		}

		// Call the existing UpdateDocumentMetadata function
		err := c.UpdateDocumentMetadata(updateReq)
		if err != nil {
			log.Printf("Failed to update metadata for Dify document %s after removing Confluence ID %s: %v", documentID, confluenceIDToRemove, err)
			// Rollback internal map? Or just return error? Let's return error.
			// Rollback the change in the internal map
			c.SetDocumentMetadataRecord(documentID, record) // Restore original record
			return fmt.Errorf("failed to update metadata for document %s: %w", documentID, err)
		}
		return nil // Metadata updated, no deletion needed
	}

	// --- If no IDs remain, proceed with actual deletion ---
	log.Printf("Confluence ID %s is the last association for Dify document %s. Proceeding with deletion.", confluenceIDToRemove, documentID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Increased timeout slightly
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent { // Allow 204 No Content as success
		// Attempt to read the error body
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		log.Printf("Document deletion failed. Status: %d, Body: %s", resp.StatusCode, bodyString)
		return fmt.Errorf("unexpected status code %d during deletion of %s. Body: %s", resp.StatusCode, documentID, bodyString)
	}

	// Successfully deleted, remove from internal map
	delete(c.metaMapping, documentID)
	log.Printf("Successfully deleted Dify document %s.", documentID)
	return nil
}
