package dify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
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

// Job represents a keyword update job for a document
type Job struct {
	DocumentID string   // Document ID to update
	Keywords   []string // Keywords to update for document segments
}

type JobChannels struct {
	Jobs chan Job
}

// InitKeywordProcessor initializes and starts the keyword processing workers.
// numWorkers: the number of concurrent workers to process keyword updates.
// jobQueueSize: the buffer size of the job channel.
func (c *Client) InitKeywordProcessor(jobQueueSize int) error {
	if jobQueueSize < 0 { // 0 is acceptable for unbuffered, but usually >0 is better
		return fmt.Errorf("jobQueueSize must be non-negative")
	}

	c.keywordChannels = &JobChannels{
		Jobs: make(chan Job, jobQueueSize),
	}

	// Support single worker. numWorkers is validated to be positive, but only one worker is started.
	log.Printf("Starting 1 keyword worker with queue size %d.", jobQueueSize)
	c.keywordWg.Add(1)
	go c.workerKeywords(c.keywordChannels.Jobs, &c.keywordWg)
	return nil
}

// worker processes keyword update jobs from the channel
func (c *Client) workerKeywords(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		if err := c.updateDocumentKeywords(&job); err != nil {
			log.Printf("error processing keyword update job for document %s: %v", job.DocumentID, err)
		}
	}
}

// AddKeywordJob submits a job to update keywords for a document.
// This function will block if the job queue is full.
func (c *Client) AddKeywordJob(documentID string, keywords []string) error {
	if c.keywordChannels == nil || c.keywordChannels.Jobs == nil {
		return fmt.Errorf("keyword processor not initialized. Call InitKeywordProcessor first")
	}
	job := Job{
		DocumentID: documentID,
		Keywords:   keywords,
	}
	c.keywordChannels.Jobs <- job
	return nil
}

// CloseKeywordWorker gracefully shuts down the keyword processing workers.
func (c *Client) CloseKeywordWorker() {
	if c.keywordChannels != nil && c.keywordChannels.Jobs != nil {
		close(c.keywordChannels.Jobs)
	}
	c.keywordWg.Wait()
	log.Println("Keyword processor shut down.")
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
func (c *Client) updateDocumentKeywords(job *Job) error {
	ctx := context.Background()
	segments, err := c.fetchDocumentSegments(ctx, job.DocumentID, 1, 100) // Start from page 1
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	for _, segment := range segments {
		// Merge existing and new keywords
		mergedKeywords := c.mergeKeywords(segment.Keywords, job.Keywords)

		// Only update if keywords have actually changed to avoid unnecessary API calls.
		if c.keywordsHaveChanged(segment.Keywords, mergedKeywords) {
			segmentReq := SegmentReq{
				Content:               segment.Content,
				Answer:                segment.Answer, // Preserve existing answer
				Keywords:              mergedKeywords,
				Enabled:               segment.Enabled,
				RegenerateChildChunks: false, // Assuming this is the desired default
			}
			if err := c.updateSegmentKeywords(ctx, job.DocumentID, segment.ID, segmentReq); err != nil {
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
func (c *Client) updateSegmentKeywords(ctx context.Context, documentID, segmentID string, segment SegmentReq) error {
	// Validate input parameters
	if documentID == "" || segmentID == "" {
		return fmt.Errorf("documentID and segmentID must not be empty")
	}

	url := fmt.Sprintf("%s/documents/%s/segments/%s", c.baseURL, documentID, segmentID)

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
