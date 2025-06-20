package dify

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// Supported field types for metadata
// Added fields for local folder sync: doc_id, original_path, last_modified, content_hash
var metaFields = []string{
	"title", "url", "source_type", "type", "key", "download", "id", "last_modified", "last_modified_date", "xxh3", "hash",
}

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
			strType := "string"
			if field == "last_modified" {
				// Skip creating the 'id' field as it is a built-in field
				strType = "number"
			}
			response, err := c.CreateMetadata(ctx, CreateMetadataRequest{
				Type: strType,
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
	url := fmt.Sprintf("%s/metadata/built-in/%s", c.baseURL, action)

	req, err := c.newRequest(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

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
	url := fmt.Sprintf("%s/metadata", c.baseURL)

	req, err := c.newRequest(ctx, "POST", url, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

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
	url := fmt.Sprintf("%s/metadata", c.baseURL)

	req, err := c.newRequest(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

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

// UpdateDocumentMetadataByFields updates document metadata using field names and values
func (c *Client) updateDocumentMetadataByRequest(request UpdateDocumentMetadataRequest) error {
	url := fmt.Sprintf("%s/documents/metadata", c.baseURL)

	// Create context with 2 minute timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel() // Ensure context is canceled to release resources

	req, err := c.newRequest(ctx, "POST", url, request)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

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

// UpdateDocumentMetadata updates document metadata in Dify API and internal cache
func (c *Client) UpdateDocumentMetadata(documentID, source string, params DocumentMetadataRecord) error {
	if record, exists := c.GetDocumentMetadataRecord(documentID); exists {
		if record.When != "" && utils.BeforeRFC3339Times(params.When, record.When) {
			params.When = record.When // Use existing value if the new one is older
		}
	}
	// 1. Calculate final Confluence IDs based on params.ConfluenceIDToAdd and existing internal state
	finalConfluenceIDsValue := c.calculateFinalConfluenceIDs(documentID, params.IDToAdd)

	// 2. Prepare metadata for API call using the fields from the params struct
	metadataToUpdate := c.buildApiMetadataPayload(params, finalConfluenceIDsValue, source)

	// 3. Perform the API call if there's anything to update
	var apiErr error
	if len(metadataToUpdate) > 0 {
		updateMetadataRequest := UpdateDocumentMetadataRequest{
			OperationData: []DocumentOperation{
				{
					DocumentID:   documentID,
					MetadataList: metadataToUpdate,
				},
			},
		}
		apiErr = c.updateDocumentMetadataByRequest(updateMetadataRequest)
		if apiErr != nil {
			log.Printf("failed to update metadata via API for Dify document %s: %v", documentID, apiErr)
			// Return the error immediately, do not update internal cache on API failure
			return fmt.Errorf("failed to update Dify metadata via API: %w", apiErr)
		}
	} else {
		log.Printf("No metadata fields configured or provided for API update for Dify document %s", documentID)
		// No API call needed, proceed to update internal cache
	}

	// 4. Update the client's internal record *only after* successful API call (or if no call was needed)
	recordToStore, _ := c.GetDocumentMetadataRecord(documentID) // Get existing or zero-value struct

	// Update fields based on input parameters (params), ensuring DifyID is always set
	recordToStore.DifyID = documentID // Ensure DifyID is always set/correct
	// Update fields only if the corresponding value in params is non-empty
	if params.URL != "" {
		recordToStore.URL = params.URL
	}
	recordToStore.SourceType = source // Always set source type in internal cache
	if params.Type != "" {
		recordToStore.Type = params.Type
	}
	if params.Key != "" {
		recordToStore.Key = params.Key
	}
	// Use the calculated final value for ConfluenceIDs in internal storage
	recordToStore.IDs = finalConfluenceIDsValue
	if params.When != "" { // Use params.When (matches struct field name)
		recordToStore.When = params.When
	}
	if params.Xxh3 != "" { // Use params.Xxh3 (matches struct field name)
		recordToStore.Xxh3 = params.Xxh3
	}
	if params.Hash != "" { // Use params.Xxh3 (matches struct field name)
		recordToStore.Hash = params.Hash
	}
	// Note: ConfluenceIDToAdd is transient and not stored in the cache record.

	// --- Hash Mapping Update Logic ---
	// Get the old hash *before* updating the record
	oldXxh3 := ""
	if existingRecord, exists := c.GetDocumentMetadataRecord(documentID); exists {
		oldXxh3 = existingRecord.Xxh3
	}

	// Store the updated/new record in the client's cache
	c.SetDocumentMetadataRecord(documentID, recordToStore) // recordToStore is already DocumentMetadataRecord type

	// Update hashMapping if the hash has changed and is not empty
	newXxh3 := recordToStore.Xxh3 // Use the hash from the record we just stored
	if newXxh3 != "" && newXxh3 != oldXxh3 {
		// Add new mapping regardless of whether the old one was removed
		c.SetHashMapping(newXxh3, documentID)
	}
	// --- End Hash Mapping Update Logic ---

	return nil // Success
}

// buildApiMetadataPayload prepares metadata for API requests
func (c *Client) buildApiMetadataPayload(params DocumentMetadataRecord, finalConfluenceIDsValue, source string) []DocumentMetadata {
	metadataToUpdate := []DocumentMetadata{}

	// Use fields directly from the params (DocumentMetadataRecord) struct
	c.addMetadataIfValid(&metadataToUpdate, "title", params.Title)
	c.addMetadataIfValid(&metadataToUpdate, "url", params.URL)
	c.addMetadataIfValid(&metadataToUpdate, "source_type", source) // Always set source_type for API
	c.addMetadataIfValid(&metadataToUpdate, "type", params.Type)
	c.addMetadataIfValid(&metadataToUpdate, "key", params.Key)
	c.addMetadataIfValid(&metadataToUpdate, "last_modified_date", params.When) // Use params.When
	c.addMetadataIfValid(&metadataToUpdate, "xxh3", params.Xxh3)               // Use params.Xxh3
	c.addMetadataIfValid(&metadataToUpdate, "hash", params.Hash)               // Use params.Xxh3
	if t, err := utils.RFC3339ToUnix(params.When); err == nil {
		c.addMetadataIfValid(&metadataToUpdate, "last_modified", fmt.Sprintf("%d", t)) // Use params.Xxh3
	} else {
		log.Printf("failed to convert last_modified to Unix time: %v", err)
	}

	// Add Confluence ID metadata ('id' field) if configured and the final value is not empty
	metaIDFieldID := c.GetMetaID("id") // Dify's internal ID for the 'id' metadata field
	if metaIDFieldID != "" && finalConfluenceIDsValue != "" {
		metadataToUpdate = append(metadataToUpdate, DocumentMetadata{ID: metaIDFieldID, Name: "id", Value: finalConfluenceIDsValue})
	}

	return metadataToUpdate
}

// addMetadataIfValid is a helper to add metadata to a list if the field is configured in Dify and the value is not empty.
func (c *Client) addMetadataIfValid(metadataList *[]DocumentMetadata, fieldName, value string) {
	metaID := c.GetMetaID(fieldName)
	// Only add if the field is known to Dify (has an ID) and the value is non-empty
	if metaID != "" && value != "" {
		*metadataList = append(*metadataList, DocumentMetadata{ID: metaID, Name: fieldName, Value: value})
	}
}

// calculateFinalConfluenceIDs determines the correct comma-separated string of Confluence IDs.
// It considers existing IDs in the cache and the newly provided ID.
func (c *Client) calculateFinalConfluenceIDs(documentID, newConfluenceID string) string {
	currentRecord, recordExists := c.GetDocumentMetadataRecord(documentID)
	existingIDsSet := make(map[string]struct{}) // Use a set for easier management

	// Populate the set with existing IDs from the cache
	if recordExists && currentRecord.IDs != "" {
		for _, id := range strings.Split(currentRecord.IDs, ",") {
			trimmedID := strings.TrimSpace(id)
			if trimmedID != "" {
				existingIDsSet[trimmedID] = struct{}{}
			}
		}
	}

	// Add the new ID if it's provided and not empty
	trimmedNewID := strings.TrimSpace(newConfluenceID)
	if trimmedNewID != "" {
		existingIDsSet[trimmedNewID] = struct{}{} // Add to set (duplicates are handled automatically)
	}

	// Convert the set back to a sorted comma-separated string for consistency
	finalIDs := make([]string, 0, len(existingIDsSet))
	for id := range existingIDsSet {
		finalIDs = append(finalIDs, id)
	}
	// Optional: Sort for deterministic order, useful for comparisons but might not be strictly necessary
	// sort.Strings(finalIDs)
	return strings.Join(finalIDs, ",")
}

// SetDocumentMetadataRecord stores or updates a DocumentMetadataRecord in the client's internal cache.
func (c *Client) SetDocumentMetadataRecord(difyID string, record DocumentMetadataRecord) {
	c.metaMutex.Lock()
	defer c.metaMutex.Unlock()

	// The record parameter is already of the correct type DocumentMetadataRecord
	if c.metaMapping == nil {
		c.metaMapping = make(map[string]DocumentMetadataRecord)
	}

	record.DifyID = difyID // Ensure the DifyID is set correctly in the record being stored
	c.metaMapping[difyID] = record
}
