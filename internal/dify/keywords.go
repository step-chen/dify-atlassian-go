package dify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
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
	if err = c.updateDocumentKeywords(difyID); err != nil {
		return fmt.Errorf("error updating keywords for document %s: %v", difyID, err)
	}
	c.removeKeywordsCache(difyID)
	return nil
}

func (c *Client) setKeywordsCache(difyID string, segKeywords map[int][]string) {
	c.docKeywordsMutex.Lock()
	defer c.docKeywordsMutex.Unlock()
	if c.docKeywords == nil {
		c.docKeywords = make(map[string]map[int][]string)
	}

	if len(segKeywords) == 0 {
		delete(c.docKeywords, difyID)
		return
	}

	c.docKeywords[difyID] = segKeywords
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

func (c *Client) getKeywordsCache(difyID string, segOrderID int) []string {
	c.docKeywordsMutex.RLock()
	defer c.docKeywordsMutex.RUnlock()

	if c.docKeywords == nil {
		return nil
	}

	segmentMap, docExists := c.docKeywords[difyID]
	if !docExists || segmentMap == nil {
		return nil
	}

	keywords, segExists := segmentMap[segOrderID]
	if !segExists {
		globalKeywords, globalExists := segmentMap[-1]
		if !globalExists {
			return nil
		}
		keywords = globalKeywords
	}
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

	var keywordsToProcess []string
	for difyID := range c.docKeywords {
		keywordsToProcess = append(keywordsToProcess, difyID)
	}

	c.docKeywordsMutex.Unlock()

	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if len(keywordsToProcess) == 0 {
			log.Println("No keywords to process in current attempt.")
			break
		}

		log.Printf("Attempt %d of %d: Starting to finalize keywords for %d documents...", attempt+1, maxRetries+1, len(keywordsToProcess))
		successfulUpdates := 0
		failedKeywords := []string{}

		for _, difyID := range keywordsToProcess {
			err := c.updateDocumentKeywords(difyID)
			if err != nil {
				log.Printf("Attempt %d: Error finalizing keywords for document %s: %v. Will retry if attempts remain.", attempt+1, difyID, err)
				failedKeywords = append(failedKeywords, difyID)
			} else {
				successfulUpdates++
				c.removeKeywordsCache(difyID)
			}
		}

		log.Printf("Attempt %d finished. Successful: %d, Failed: %d.", attempt+1, successfulUpdates, len(failedKeywords))

		if len(failedKeywords) == 0 {
			log.Println("All keywords finalized successfully in this attempt.")
			break
		}

		keywordsToProcess = failedKeywords

		if attempt < maxRetries {
			log.Printf("Retrying %d failed keyword updates...", len(keywordsToProcess))
		} else {
			log.Printf("Max retries reached. %d keyword updates still failed.", len(keywordsToProcess))
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

// updateDocumentKeywords updates keywords for all segments of a document
func (c *Client) updateDocumentKeywords(difyID string) error {
	ctx := context.Background()
	segments, err := c.fetchDocumentSegments(ctx, difyID, 1, 100) // Start from page 1
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	nSeg := 0
	for i, segment := range segments {
		keywords := c.getKeywordsCache(difyID, i-nSeg)
		if c.segEofTag != "" {
			if strings.HasSuffix(segment.Content, c.segEofTag) {
				segment.Content = strings.TrimSuffix(segment.Content, c.segEofTag)
			} else {
				nSeg++
			}
		}

		// Merge existing keywords on the segment with the new keywords determined for this segment
		// mergedKeywords := c.mergeKeywords(segment.Keywords, keywords)

		if c.keywordsHaveChanged(segment.Keywords, keywords) {
			segmentReq := SegmentReq{
				Content:               segment.Content,
				Answer:                segment.Answer, // Preserve existing answer
				Keywords:              keywords,
				Enabled:               segment.Enabled,
				RegenerateChildChunks: false, // Assuming this is the desired default
			}
			if err := c.updateSegmentKeywords(ctx, difyID, segment.ID, segmentReq); err != nil {
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
