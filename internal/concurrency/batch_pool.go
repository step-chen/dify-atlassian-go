package concurrency

import (
	"log"
	"sync"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
)

type BatchPool struct {
	mu            sync.Mutex
	cond          *sync.Cond
	batches       map[string]time.Time
	maxSize       int
	statusChecker func(spaceKey, confluenceID, batchID string, op confluence.ContentOperation) (string, error)
}

func NewBatchPool(maxSize int, statusChecker func(spaceKey, confluenceID, batchID string, op confluence.ContentOperation) (string, error)) *BatchPool {
	bp := &BatchPool{
		batches:       make(map[string]time.Time),
		maxSize:       maxSize,
		statusChecker: statusChecker,
	}
	bp.cond = sync.NewCond(&bp.mu)
	return bp
}

func (bp *BatchPool) Add(spaceKey, confluenceID, batchID string, op confluence.ContentOperation) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for len(bp.batches) >= bp.maxSize {
		bp.cond.Wait()
	}

	bp.batches[batchID] = time.Now()
	go bp.monitorBatch(spaceKey, confluenceID, batchID, op)
}

func (bp *BatchPool) monitorBatch(spaceKey, confluenceID, batchID string, op confluence.ContentOperation) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status, err := bp.statusChecker(spaceKey, confluenceID, batchID, op)
		if status == "completed" {
			bp.remove(batchID)
			return
		}
		if err != nil {
			log.Printf("Failed to check status of batch %s: %v", batchID, err)
			return
		}
	}
}

func (bp *BatchPool) remove(batchID string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	delete(bp.batches, batchID)
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
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.batches)
}

// MaxSize returns the maximum capacity of the batch pool
func (bp *BatchPool) MaxSize() int {
	return bp.maxSize
}
