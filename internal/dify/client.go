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
	baseURL     string               // API endpoint base URL
	apiKey      string               // Decrypted API key for authentication
	datasetID   string               // Target dataset ID for operations
	httpClient  *http.Client         // HTTP client with default timeout
	config      *config.Config       // Application configuration
	meta        map[string]MetaField // map[metaName]MetaField
	metaMapping map[string]string    // Metadata map[difyID]confluenceIDs(split with ",")
}

// DatasetID returns the configured dataset ID for this client
func (c *Client) DatasetID() string {
	return c.datasetID
}

// GetConfluenceIDsForDifyID retrieves the stored comma-separated Confluence IDs for a given Dify document ID.
func (c *Client) GetConfluenceIDsForDifyID(difyID string) string {
	// Ensure metaMapping is initialized before accessing
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]string) // Or log an error if it should always be initialized
	}
	return c.metaMapping[difyID] // Returns "" if key doesn't exist
}

// SetMetaMapping updates the internal mapping for a given Dify ID.
func (c *Client) SetMetaMapping(difyID, confluenceIDs string) {
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]string)
	}
	c.metaMapping[difyID] = confluenceIDs
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
		metaMapping: make(map[string]string), // Initialize metaMapping
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

// FetchDocumentsList retrieves paginated document list
// page: Starting page number (0-based)
// limit: Number of items per page (max 100)
// Returns map of document info or error
type DocumentInfo struct {
	DifyID         string
	When           string
	IndexingStatus string
}

func (c *Client) FetchDocumentsList(page, limit int) (map[string]DocumentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	allDocuments := make(map[string]DocumentInfo)

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
			var idValue, whenValue string
			for _, meta := range doc.DocMetadata {
				if meta.Name == "id" {
					idValue = meta.Value
				} else if meta.Name == "when" {
					whenValue = meta.Value
				}
			}
			if idValue != "" {
				// Store the mapping from Dify ID to Confluence IDs
				c.metaMapping[doc.ID] = idValue

				// Split the comma-separated IDs for allDocuments map (maps Confluence ID -> Dify Info)
				ids := strings.Split(idValue, ",")
				for _, id := range ids {
					trimmedID := strings.TrimSpace(id)
					if trimmedID != "" {
						allDocuments[trimmedID] = DocumentInfo{
							DifyID: doc.ID,
							When:   whenValue,
						}
					}
				}
			}
		}

		if !response.HasMore {
			break
		}

		page++
	}

	return allDocuments, nil
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
	existingIDsStr := c.GetConfluenceIDsForDifyID(documentID)
	if existingIDsStr == "" {
		// If there's no mapping, maybe the document was already deleted or never mapped?
		// Attempt deletion anyway, or log a warning. Let's try deleting.
		log.Printf("Warning: No Confluence ID mapping found for Dify document %s during deletion attempt for Confluence ID %s. Attempting direct deletion.", documentID, confluenceIDToRemove)
		// Fall through to actual deletion logic below.
	}

	existingIDs := strings.Split(existingIDsStr, ",")
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
	if !found && existingIDsStr != "" {
		log.Printf("Warning: Confluence ID %s not found in metadata for Dify document %s (%s). Proceeding with potential deletion.", confluenceIDToRemove, documentID, existingIDsStr)
		// Decide if we should still delete. Let's assume yes for now.
	}

	// If there are other IDs remaining after removal
	if len(remainingIDs) > 0 {
		log.Printf("Dify document %s has other associated Confluence IDs. Updating metadata instead of deleting.", documentID)
		newIDsStr := strings.Join(remainingIDs, ",")
		c.SetMetaMapping(documentID, newIDsStr) // Update internal map

		// Prepare metadata update request for Dify API
		metaIDFieldID := c.GetMetaID("id")
		if metaIDFieldID == "" {
			// This should not happen if InitMetadata ran correctly
			return fmt.Errorf("critical error: metadata field 'id' not found in client config for dataset %s", c.datasetID)
		}

		updateReq := UpdateDocumentMetadataRequest{
			OperationData: []DocumentOperation{
				{
					DocumentID: documentID,
					MetadataList: []DocumentMetadata{
						{
							ID:    metaIDFieldID, // Use the actual field ID from config
							Name:  "id",          // Name is likely informational here, ID matters
							Value: newIDsStr,
						},
					},
				},
			},
		}
		// Call the existing UpdateDocumentMetadata function (from meta.go)
		err := c.UpdateDocumentMetadata(updateReq)
		if err != nil {
			log.Printf("Failed to update metadata for Dify document %s after removing Confluence ID %s: %v", documentID, confluenceIDToRemove, err)
			// Rollback internal map? Or just return error? Let's return error.
			// c.SetMetaMapping(documentID, existingIDsStr) // Optional rollback
			return fmt.Errorf("failed to update metadata for document %s: %w", documentID, err)
		}
		log.Printf("Successfully updated metadata for Dify document %s, removed association with Confluence ID %s.", documentID, confluenceIDToRemove)
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
