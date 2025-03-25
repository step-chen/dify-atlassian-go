package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// type: page, attachment
// source_type: confluence
var metaFields = []string{"url", "source_type", "type", "space_key", "title", "download", "id", "when"}

type MetaField struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type Metadata struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	UseCount int    `json:"use_count"`
}

type MetadataResponse struct {
	Metadata            []Metadata `json:"doc_metadata"`
	BuiltInFieldEnabled bool       `json:"built_in_field_enabled"`
}

func (c *Client) InitMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	meta, err := c.GetDatasetMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to get dataset metadata: %w", err)
	}
	if !meta.BuiltInFieldEnabled {
		if err := c.EnableBuiltInMetadata(ctx, true); err != nil {
			return fmt.Errorf("failed to enable built-in metadata: %w", err)
		}
	}

	c.meta = make(map[string]MetaField)
	for _, m := range meta.Metadata {
		c.meta[m.Name] = MetaField{
			ID:   m.ID,
			Type: m.Type,
		}
	}

	// Create missing metadata fields
	for _, field := range metaFields {
		if _, exists := c.meta[field]; !exists {
			response, err := c.CreateMetadata(ctx, CreateMetadataRequest{
				Type: "string",
				Name: field,
			})
			if err != nil {
				return fmt.Errorf("failed to create metadata field %s: %w", field, err)
			}
			// Add newly created metadata to c.meta
			c.meta[field] = MetaField{
				ID:   response.ID,
				Type: response.Type,
			}
		}
	}

	return nil
}

// Get returns the ID of the MetaField with the given name
func (c *Client) GetMetaID(name string) string {
	if field, exists := c.meta[name]; exists {
		return field.ID
	}
	return ""
}

func (c *Client) EnableBuiltInMetadata(ctx context.Context, enable bool) error {
	action := "enable"
	if !enable {
		action = "disable"
	}
	url := fmt.Sprintf("%s/datasets/%s/metadata/built-in/%s", c.baseURL, c.datasetID, action)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

type CreateMetadataRequest struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type CreateMetadataResponse struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

func (c *Client) CreateMetadata(ctx context.Context, metadata CreateMetadataRequest) (*CreateMetadataResponse, error) {
	url := fmt.Sprintf("%s/datasets/%s/metadata", c.baseURL, c.datasetID)

	jsonData, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Println("create meta data for dataset request failed: %w", err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response CreateMetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

func (c *Client) GetDatasetMetadata(ctx context.Context) (*MetadataResponse, error) {
	url := fmt.Sprintf("%s/datasets/%s/metadata", c.baseURL, c.datasetID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var metadata MetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &metadata, nil
}

type DocumentMetadata struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	Name  string `json:"name"`
}

type DocumentOperation struct {
	DocumentID   string             `json:"document_id"`
	MetadataList []DocumentMetadata `json:"metadata_list"`
}

type UpdateDocumentMetadataRequest struct {
	OperationData []DocumentOperation `json:"operation_data"`
}

// UpdateDocumentMetadataByFields updates document metadata using field names and values
func (c *Client) UpdateDocumentMetadata(ctx context.Context, request UpdateDocumentMetadataRequest) error {
	url := fmt.Sprintf("%s/datasets/%s/documents/metadata", c.baseURL, c.datasetID)
	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
