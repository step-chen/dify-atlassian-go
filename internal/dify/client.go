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

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

// Client defines the Dify API client
type Client struct {
	baseURL    string         // Base URL of the Dify API
	apiKey     string         // API key for authentication
	datasetID  string         // Dataset ID to operate on
	httpClient *http.Client   // HTTP client for making requests
	config     *config.Config // Configuration
	meta       map[string]MetaField
}

// DatasetID returns the dataset ID
func (c *Client) DatasetID() string {
	return c.datasetID
}

// NewClient creates a new Dify client instance
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

// GetIndexingStatus retrieves the indexing status of a document
func (c *Client) GetIndexingStatus(ctx context.Context, spaceKey, batch string) (*IndexingStatusResponse, error) {
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
		log.Printf("unexpected status code: %d, datasetID: %s, batch: %s, url: %s", resp.StatusCode, c.datasetID, batch, url)
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var statusResponse IndexingStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &statusResponse, nil
}

// CreateDocumentByText creates a new document
func (c *Client) CreateDocumentByText(ctx context.Context, req *CreateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

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

// CreateDocumentByFile creates a new document by uploading a file
func (c *Client) CreateDocumentByFile(ctx context.Context, filePath string, req *CreateDocumentByFileRequest, title string) (*CreateDocumentResponse, error) {
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

// ListDocuments retrieves all documents in the dataset
func (c *Client) ListDocuments(ctx context.Context, page, limit int) (map[string]bool, error) {
	allDocuments := make(map[string]bool)

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
			allDocuments[doc.ID] = true
		}

		if !response.HasMore {
			break
		}

		page++
	}

	return allDocuments, nil
}

// UpdateDocumentByText updates a document by text
func (c *Client) UpdateDocumentByText(ctx context.Context, datasetID, documentID string, req *UpdateDocumentRequest) (*CreateDocumentResponse, error) {
	if req.ProcessRule.Mode == "" {
		req.ProcessRule = DefaultProcessRule(c.config)
	}

	requestBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/datasets/%s/documents/%s/update-by-text", c.baseURL, datasetID, documentID)
	updateRequest, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	updateRequest.Header.Set("Content-Type", "application/json")
	updateRequest.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(updateRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response CreateDocumentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// DeleteDocument deletes a document by ID
func (c *Client) DeleteDocument(ctx context.Context, documentID string) error {
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
