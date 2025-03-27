package concurrency

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
)

// NotFoundErrorInfo holds information about a document not found error
type NotFoundErrorInfo struct {
	SpaceKey     string
	ConfluenceID string
	Title        string
}

type BatchPool struct {
	mu             sync.Mutex
	cond           *sync.Cond
	batches        map[string]time.Time
	maxSize        int
	statusChecker  func(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error)
	total          int
	remain         int
	total_len      int
	notFoundErrors map[string]NotFoundErrorInfo // Map to store not found errors, keyed by DifyID
}

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

func (bp *BatchPool) Add(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for len(bp.batches) >= bp.maxSize {
		bp.cond.Wait()
	}

	bp.batches[batch] = time.Now()
	go bp.monitorBatch(spaceKey, confluenceID, title, batch, op)
}

func (bp *BatchPool) monitorBatch(spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status, err := bp.statusChecker(spaceKey, confluenceID, title, batch, op)
		if status == "completed" {
			log.Printf("% *d/%d successfully indexing Dify document %s for [%s] content [%s]", bp.GetTotalLen(), bp.GetCompleted(), bp.GetTotal(), op.DifyID, spaceKey, title)
			bp.SetRemain(bp.remain - 1)
			bp.remove(batch)
			return
		} else if status == "deleted" {
			log.Printf("deleted document %s for [%s] content [%s] due to timeout", op.DifyID, spaceKey, title)
			bp.remove(batch)
			return
		} else if err != nil {
			if err.Error() == "unexpected status code: 404" {
				log.Printf("document %s for [%s] content [%s] batch [%s] not found", op.DifyID, spaceKey, title, batch)
				bp.recordNotFoundError(op.DifyID, spaceKey, confluenceID, title) // Record the error
				bp.SetRemain(bp.remain - 1)
				bp.remove(batch)
				return
			}
			log.Printf("failed to check status of batch %s: %v", batch, err)
			// Consider if other errors should also decrement remain count or be handled differently
			// For now, just log and let the ticker retry or timeout handle it.
			// If we return here, the batch might get stuck if the error is persistent but not a 404.
			// Depending on desired behavior, might need bp.remove(batch) and bp.SetRemain(bp.remain - 1) here too.
			return // Returning here to avoid potential infinite loops on persistent errors
		}
	}
}

func (bp *BatchPool) remove(batch string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	delete(bp.batches, batch)
	bp.cond.Signal()
}

// WaitForAvailable waits until there is available space in the batch pool
func (bp *BatchPool) WaitForAvailable() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for len(bp.batches) >= bp.maxSize {
		bp.cond.Wait()
	}
}

// Size returns the current number of batches in the pool
func (bp *BatchPool) Size() int {
	return len(bp.batches)
}

// MaxSize returns the maximum capacity of the batch pool
func (bp *BatchPool) MaxSize() int {
	return bp.maxSize
}

// SetTotal sets the total number of operations
func (bp *BatchPool) SetTotal(total int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.total = total
	totalStr := strconv.Itoa(bp.total)
	bp.total_len = len(totalStr)
}

// SetRemain sets the remaining number of operations
func (bp *BatchPool) SetRemain(remain int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.remain = remain
}

// GetTotalLen returns the length of the total operations count as a string
func (bp *BatchPool) GetTotalLen() int {
	return bp.total_len
}

// GetCompleted returns the completed number of operations
func (bp *BatchPool) GetCompleted() int {
	return bp.total - bp.remain
}

// GetTotal returns the total number of operations
func (bp *BatchPool) GetTotal() int {
	return bp.total
}

// recordNotFoundError stores the details of a 404 error
func (bp *BatchPool) recordNotFoundError(difyID, spaceKey, confluenceID, title string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.notFoundErrors[difyID] = NotFoundErrorInfo{
		SpaceKey:     spaceKey,
		ConfluenceID: confluenceID,
		Title:        title,
	}
}

// LogNotFoundErrors prints all recorded 404 errors
func (bp *BatchPool) LogNotFoundErrors() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

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
