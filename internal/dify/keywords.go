package dify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// SegmentResp represents a document segment from Dify API
type SegmentResp struct {
	ID       string   `json:"id"`
	Content  string   `json:"content"`
	Keywords []string `json:"keywords"`
	Answer   string   `json:"answer,omitempty"` // New field for storing AI answers
	Enabled  bool     `json:"enabled"`
}

// Define the inner segment structure matching API spec
type SegmentReq struct {
	Content               string   `json:"content"`
	Answer                string   `json:"answer"`
	Keywords              []string `json:"keywords"`
	Enabled               bool     `json:"enabled"`
	RegenerateChildChunks bool     `json:"regenerate_child_chunks,omitempty"`
}

func (c *Client) UpdateKeywords(difyID string) (err error) {
	keywords := c.getKeywordsCache(difyID)
	if len(keywords) > 0 {
		if err = c.updateDocumentKeywords(difyID, keywords); err != nil {
			return fmt.Errorf("error updating keywords for document %s: %v", difyID, err)
		}
		c.removeKeywordsCache(difyID)
	}

	return nil
}

func (c *Client) setKeywordsCache(difyID string, keywords []string) {
	c.docKeywordsMutex.Lock()
	defer c.docKeywordsMutex.Unlock()
	if c.docKeywords == nil { // Ensure the map is initialized
		c.docKeywords = make(map[string][]string)
	}
	if len(keywords) == 0 {
		delete(c.docKeywords, difyID)
	} else {
		c.docKeywords[difyID] = keywords
	}
}

// removeKeywordsCache removes the keywords for a given Dify document ID from the cache.
func (c *Client) removeKeywordsCache(difyID string) {
	c.docKeywordsMutex.Lock()
	defer c.docKeywordsMutex.Unlock()
	if c.docKeywords == nil {
		return // Nothing to do if the map isn't initialized
	}
	delete(c.docKeywords, difyID)
}

func (c *Client) getKeywordsCache(difyID string) []string {
	c.docKeywordsMutex.RLock()
	defer c.docKeywordsMutex.RUnlock()
	if c.docKeywords == nil {
		return nil
	}
	keywords, exists := c.docKeywords[difyID]
	if !exists {
		return nil
	}
	// Return a copy to prevent external modification of the internal slice
	keywordsCopy := make([]string, len(keywords))
	copy(keywordsCopy, keywords)
	return keywordsCopy
}

func (c *Client) finalizeKeywords() {
	c.docKeywordsMutex.Lock()
	if len(c.docKeywords) == 0 {
		c.docKeywordsMutex.Unlock()
		log.Println("No document keywords to finalize.")
		return
	}

	// Create a snapshot of the current keywords to process
	keywordsToProcess := make(map[string][]string, len(c.docKeywords))
	for docID, kw := range c.docKeywords {
		keywordsToProcess[docID] = kw
	}

	// Clear the main map; failed items will be added back.
	c.docKeywords = make(map[string][]string)
	c.docKeywordsMutex.Unlock()

	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if len(keywordsToProcess) == 0 {
			log.Println("No keywords to process in current attempt.")
			break
		}

		log.Printf("Attempt %d of %d: Starting to finalize keywords for %d documents...", attempt+1, maxRetries+1, len(keywordsToProcess))
		successfulUpdatesThisAttempt := 0
		failedKeywordsThisAttempt := make(map[string][]string)

		for docID, keywords := range keywordsToProcess {
			// Assuming setDocumentKeywords ensures no empty keyword slices are stored if that means "delete".
			// If keywords is empty here, updateDocumentKeywords will handle it (currently merges with existing).
			log.Printf("Attempt %d: Finalizing keywords for document %s: %v", attempt+1, docID, keywords)
			err := c.updateDocumentKeywords(docID, keywords)
			if err != nil {
				log.Printf("Attempt %d: Error finalizing keywords for document %s: %v. Will retry if attempts remain.", attempt+1, docID, err)
				failedKeywordsThisAttempt[docID] = keywords
			} else {
				successfulUpdatesThisAttempt++
				log.Printf("Attempt %d: Successfully finalized keywords for document %s.", attempt+1, docID)
			}
		}

		log.Printf("Attempt %d finished. Successful: %d, Failed: %d.", attempt+1, successfulUpdatesThisAttempt, len(failedKeywordsThisAttempt))

		if len(failedKeywordsThisAttempt) == 0 {
			log.Println("All keywords finalized successfully in this attempt.")
			break // All done
		}

		keywordsToProcess = failedKeywordsThisAttempt // Prepare for next retry

		if attempt < maxRetries {
			log.Printf("Retrying %d failed keyword updates...", len(keywordsToProcess))
			// Optional: Add a small delay before retrying
			// time.Sleep(time.Second * time.Duration(attempt+1))
		} else {
			log.Printf("Max retries reached. %d keyword updates still failed.", len(keywordsToProcess))
			// Re-queue the remaining failed keywords
			for docID, keywords := range keywordsToProcess {
				c.setKeywordsCache(docID, keywords)
			}
		}
	}
}

// GetDocumentSegments retrieves all segments for a document with pagination support
func (c *Client) fetchDocumentSegments(ctx context.Context, docID string, page, limit int) ([]SegmentResp, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var allSegments []SegmentResp

	if page < 0 {
		page = 0
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}

	for {
		req, err := c.newRequest(
			ctx,
			"GET",
			fmt.Sprintf("%s/documents/%s/segments?page=%d&limit=%d", c.baseURL, docID, page, limit),
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create segments request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to get segments: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("failed to get segments: status %d, response: %s", resp.StatusCode, string(body))
		}

		var response struct {
			Data    []SegmentResp `json:"data"`
			HasMore bool          `json:"has_more"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, fmt.Errorf("failed to decode segments response: %w", err)
		}

		allSegments = append(allSegments, response.Data...)

		if !response.HasMore {
			break
		}

		page++
	}

	return allSegments, nil
}

// mergeKeywords combines two keyword slices without duplicates
func (c *Client) mergeKeywords(existing, new []string) []string {
	keywordMap := make(map[string]struct{})
	for _, kw := range existing {
		keywordMap[kw] = struct{}{}
	}
	for _, kw := range new {
		keywordMap[kw] = struct{}{}
	}

	result := make([]string, 0, len(keywordMap))
	for kw := range keywordMap {
		result = append(result, kw)
	}
	return result
}

// updateDocumentKeywords updates keywords for all segments of a document
func (c *Client) updateDocumentKeywords(docId string, keywords []string) error {
	ctx := context.Background()
	segments, err := c.fetchDocumentSegments(ctx, docId, 1, 100) // Start from page 1
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	for _, segment := range segments {
		// Merge existing and new keywords
		mergedKeywords := c.mergeKeywords(segment.Keywords, keywords)

		// Only update if keywords have actually changed to avoid unnecessary API calls.
		if c.keywordsHaveChanged(segment.Keywords, mergedKeywords) {
			segmentReq := SegmentReq{
				Content:               segment.Content,
				Answer:                segment.Answer, // Preserve existing answer
				Keywords:              mergedKeywords,
				Enabled:               segment.Enabled,
				RegenerateChildChunks: false, // Assuming this is the desired default
			}
			if err := c.updateSegmentKeywords(ctx, docId, segment.ID, segmentReq); err != nil {
				return fmt.Errorf("failed to update keywords for segment %s: %w", segment.ID, err)
			}
		}
	}

	return nil
}

// keywordsHaveChanged checks if newKeywords differs from currentKeywords.
// Considers both count and the unique set of keywords. Assumes newKeywords is unique.
func (c *Client) keywordsHaveChanged(currentKeywords, newKeywords []string) bool {
	if len(currentKeywords) != len(newKeywords) {
		return true // Different counts mean change.
	}

	if len(newKeywords) == 0 {
		return false // Both are empty (due to previous check), no change.
	}

	// Counts are equal and non-zero. Compare unique sets.
	currentSet := make(map[string]struct{}, len(currentKeywords))
	for _, kw := range currentKeywords {
		currentSet[kw] = struct{}{}
	}

	// If unique count of current keywords differs from newKeywords count (which is unique),
	// then there's a change.
	if len(currentSet) != len(newKeywords) {
		return true
	}

	for _, kw := range newKeywords {
		if _, exists := currentSet[kw]; !exists {
			return true // A new keyword is not in the current set.
		}
	}
	return false // Keywords are effectively the same.
}

// UpdateSegmentKeywords updates a document segment's child chunk with new content.
func (c *Client) updateSegmentKeywords(ctx context.Context, docID, segmentID string, segment SegmentReq) error {
	// Validate input parameters
	if docID == "" || segmentID == "" {
		return fmt.Errorf("documentID and segmentID must not be empty")
	}

	url := fmt.Sprintf("%s/documents/%s/segments/%s", c.baseURL, docID, segmentID)

	// Define the full request body structure
	type segmentRequest struct {
		Segment *SegmentReq `json:"segment"`
	}

	// Create request with proper timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := c.newRequest(ctxWithTimeout, "POST", url, segmentRequest{Segment: &segment})
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request with retry mechanism
	var resp *http.Response
	for attempt := 0; attempt < 3; attempt++ {
		resp, err = c.httpClient.Do(req)
		if err == nil && resp.StatusCode != http.StatusTooManyRequests {
			break
		}
		// If we got rate limited or network error, wait and retry
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to update segment child chunk: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		// Read response body for better error diagnostics
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update segment child chunk: status %d, response: %s",
			resp.StatusCode, string(body))
	}

	return nil
}
