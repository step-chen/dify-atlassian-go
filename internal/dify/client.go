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
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

// Client handles communication with the Dify API
type Client struct {
	baseURL    string               // API endpoint base URL
	apiKey     string               // Decrypted API key for authentication
	datasetID  string               // Target dataset ID for operations
	httpClient *http.Client         // HTTP client with default timeout
	config     *config.Config       // Application configuration
	meta       map[string]MetaField // Metadata fields mapping
}

// DatasetID returns the configured dataset ID for this client
func (c *Client) DatasetID() string {
	return c.datasetID
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
		baseURL:    baseURL,
		apiKey:     decryptedKey,
		datasetID:  datasetID,
		httpClient: &http.Client{},
		config:     cfg,
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
				allDocuments[idValue] = DocumentInfo{
					DifyID: doc.ID,
					When:   whenValue,
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

// DeleteDocument removes document from dataset
// ctx: Context for request cancellation
// documentID: ID of document to delete
// Returns error if deletion fails
func (c *Client) DeleteDocument(documentID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/datasets/%s/documents/%s", c.baseURL, c.datasetID, documentID)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
