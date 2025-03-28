package concurrency

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
)

// NotFoundErrorInfo stores details of 404 errors
type NotFoundErrorInfo struct {
	datasetID    string
	SpaceKey     string // Confluence space key
	ConfluenceID string // Confluence document ID
	Title        string // Document title
}

// BatchPool manages concurrent batch processing
type BatchPool struct {
	mu             sync.Mutex                                                                                        // Mutex for thread safety
	cond           *sync.Cond                                                                                        // Condition variable for waiting
	batches        map[string]time.Time                                                                              // Active batches with timestamps
	maxSize        int                                                                                               // Maximum concurrent batches
	statusChecker  func(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error) // Status check callback
	total          map[string]int                                                                                    // Total operations count
	remain         map[string]int                                                                                    // Remaining operations count
	total_len      int                                                                                               // Length of total operations as string
	notFoundErrors map[string]NotFoundErrorInfo                                                                      // Map of 404 errors by DifyID
}

// NewBatchPool creates a new batch processing pool
// maxSize: Maximum concurrent batches
// statusChecker: Callback function to check batch status
// Returns initialized BatchPool
func NewBatchPool(maxSize int, statusChecker func(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error)) *BatchPool {
	bp := &BatchPool{
		batches:        make(map[string]time.Time),
		maxSize:        maxSize,
		statusChecker:  statusChecker,
		notFoundErrors: make(map[string]NotFoundErrorInfo), // Initialize the map
	}
	bp.cond = sync.NewCond(&bp.mu)
	return bp
}

// Add adds a new batch to the pool and starts monitoring
// spaceKey: Confluence space key
// confluenceID: Confluence document ID
// title: Document title
// batch: Batch ID
// op: Content operation details
func (bp *BatchPool) Add(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for len(bp.batches) >= bp.maxSize {
		bp.cond.Wait()
	}

	bp.batches[batch] = time.Now()
	go bp.monitorBatch(spaceKey, confluenceID, title, batch, op)
}

// monitorBatch periodically checks batch status until completion
// spaceKey: Confluence space key
// confluenceID: Confluence document ID
// title: Document title
// batch: Batch ID
// op: Content operation details
func (bp *BatchPool) monitorBatch(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status, err := bp.statusChecker(spaceKey, confluenceID, title, batch, op)
		if status == "completed" {
			log.Printf("%s successfully indexing Dify document %s for content [%s]", bp.ProgressString(spaceKey), op.DifyID, title)
			bp.ReduceRemain(spaceKey)
			bp.remove(batch)
			return
		} else if status == "deleted" {
			log.Printf("[%s] deleted Dify document %s for content [%s] due to timeout", spaceKey, op.DifyID, title)
			bp.remove(batch)
			return
		} else if err != nil {
			if err.Error() == "unexpected status code: 404" {
				log.Printf("[%s] indexing status Dify document %s batch %s for content [%s] not found", spaceKey, op.DifyID, title, batch)
				bp.recordNotFoundError(op.DifyID, spaceKey, confluenceID, title) // Record the error
				bp.ReduceRemain(spaceKey)
				bp.remove(batch)
				return
			}
			log.Printf("[%s] failed to check indexing status Dify document %s batch %s for content [%s]: %v", spaceKey, op.DifyID, title, batch, err)
			// Consider if other errors should also decrement remain count or be handled differently
			// For now, just log and let the ticker retry or timeout handle it.
			// If we return here, the batch might get stuck if the error is persistent but not a 404.
			// Depending on desired behavior, might need bp.remove(batch) and bp.SetRemain(bp.remain - 1) here too.
			return // Returning here to avoid potential infinite loops on persistent errors
		}
	}
}

// remove deletes a batch from the pool and signals waiting goroutines
// batch: Batch ID to remove
func (bp *BatchPool) remove(batch string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	delete(bp.batches, batch)
	bp.cond.Signal()
}

// WaitForAvailable blocks until pool has available capacity
func (bp *BatchPool) WaitForAvailable() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for len(bp.batches) >= bp.maxSize {
		bp.cond.Wait()
	}
}

// Size returns current active batch count
func (bp *BatchPool) Size() int {
	return len(bp.batches)
}

// SetTotal configures total operations count
// total: Total number of operations
func (bp *BatchPool) SetTotal(spaceKey string, total int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.total[spaceKey] = total
	bp.remain[spaceKey] = total
	totalStr := strconv.Itoa(total)
	if len(totalStr) > bp.total_len {
		bp.total_len = len(totalStr)
	}
}

// SetRemain updates remaining operations count
// remain: Number of operations remaining
func (bp *BatchPool) ReduceRemain(spaceKey string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.remain[spaceKey] = bp.remain[spaceKey] - 1
}

func (bp *BatchPool) ProgressString(spaceKey string) string {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return fmt.Sprintf("[%s] % *d/%d", spaceKey, bp.remain[spaceKey], bp.total_len, bp.total[spaceKey])
}

// recordNotFoundError logs 404 error details
// difyID: Dify document ID
// spaceKey: Confluence space key
// confluenceID: Confluence document ID
// title: Document title
func (bp *BatchPool) recordNotFoundError(difyID, spaceKey, confluenceID, title string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.notFoundErrors[difyID] = NotFoundErrorInfo{
		SpaceKey:     spaceKey,
		ConfluenceID: confluenceID,
		Title:        title,
	}
}

// LogNotFoundErrors outputs all recorded 404 errors
func (bp *BatchPool) LogNotFoundErrors() {
	if len(bp.notFoundErrors) > 0 {
		log.Println("--- Documents Not Found (404 Errors) ---")
		for difyID, info := range bp.notFoundErrors {
			log.Printf("DifyID: %s, SpaceKey: %s, ConfluenceID: %s, Title: %s",
				difyID, info.SpaceKey, info.ConfluenceID, info.Title)
		}
		log.Println("----------------------------------------")
	} else {
		log.Println("No '404 Not Found' errors were recorded during execution.")
	}
}
