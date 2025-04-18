package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config" // Import config package
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
)

type Task struct {
	SpaceKey     string
	ConfluenceID string
	Title        string
	Batch        string
	Op           confluence.ContentOperation
}

type SpaceProgress struct {
	total     atomic.Int32 // Expected total tasks for this space
	completed atomic.Int32 // Completed tasks for this space
}

type BatchPool struct {
	maxWorkers    int
	statusChecker func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error)
	taskQueue     chan Task
	workersWg     sync.WaitGroup // Waits for worker goroutines to finish on Close
	overallWg     sync.WaitGroup // Waits for all submitted tasks to complete processing
	stopOnce      sync.Once
	shutdown      chan struct{}
	progress      sync.Map // Stores map[string]*SpaceProgress for per-space tracking
	mu            sync.Mutex
	totalLen      int            // Max length of total count string across all spaces for formatting
	cfg           config.ConcCfg // Store only the concurrency config
}

func NewBatchPool(maxWorkers int, queueSize int, statusChecker func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error), concurrencyCfg config.ConcCfg) *BatchPool { // Accept ConcCfg directly
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}

	bp := &BatchPool{
		maxWorkers:    maxWorkers,
		statusChecker: statusChecker,
		taskQueue:     make(chan Task, queueSize),
		shutdown:      make(chan struct{}),
		cfg:           concurrencyCfg, // Store concurrency config
		// progress is initialized implicitly by sync.Map
	}

	bp.workersWg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go bp.worker()
	}

	return bp
}

func (bp *BatchPool) worker() {
	defer bp.workersWg.Done()
	for {
		select {
		case task, ok := <-bp.taskQueue:
			if !ok {
				return // Channel closed
			}
			bp.monitorBatch(task)
			bp.overallWg.Done() // Signal task completion *after* monitorBatch returns
		case <-bp.shutdown:
			return // Shutdown signal
		}
	}
}

func (bp *BatchPool) Add(ctx context.Context, spaceKey, confluenceID, title, content, batch string, op confluence.ContentOperation) error {
	task := Task{
		SpaceKey:     spaceKey,
		ConfluenceID: confluenceID,
		Title:        title,
		Batch:        batch,
		Op:           op,
	}

	if _, ok := bp.progress.Load(spaceKey); !ok {
		log.Printf("Warning: Adding task for spaceKey '%s' before SetTotal was called. Progress tracking might be inaccurate.", spaceKey)
		// Optionally create a default progress entry here if desired:
		// bp.progress.LoadOrStore(spaceKey, &SpaceProgress{})
	}

	bp.overallWg.Add(1) // Increment *before* sending to queue

	select {
	case bp.taskQueue <- task:
		return nil
	case <-bp.shutdown:
		bp.overallWg.Done() // Rollback WaitGroup if not submitted
		return errors.New("batch pool is shut down")
	case <-ctx.Done():
		bp.overallWg.Done() // Rollback WaitGroup if not submitted
		return ctx.Err()
	}
}

func (bp *BatchPool) monitorBatch(task Task) {
	taskTimeout := time.Duration(bp.cfg.IndexingTimeout) * time.Minute // Access IndexingTimeout directly
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second) // Keep polling interval
	defer ticker.Stop()

	logPrefix := fmt.Sprintf("Space: %s, DifyID: %s, Title: %s, Batch: %s",
		task.SpaceKey, task.Op.DifyID, task.Title, task.Batch)

	taskCompleted := false // Flag to ensure completion logic runs only once

	defer func() {
		if !taskCompleted {
			bp.MarkTaskComplete(task.SpaceKey)
			log.Printf("%s - Monitoring ended unexpectedly.", logPrefix)
		}
	}()

	for {
		select {
		case <-ticker.C:
			status, err := bp.statusChecker(ctx, task.SpaceKey, task.ConfluenceID, task.Title, task.Batch, task.Op)
			progressStr := bp.ProgressString(task.SpaceKey) // Get current progress string

			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					log.Printf("[CANCELLED] %s - %s Monitoring cancelled or timed out internally: %v", progressStr, logPrefix, err)
					// Let the outer context timeout handle the final state
				} else {
					log.Printf("[ERROR] %s - %s Failed status check: %v. Retrying...", progressStr, logPrefix, err)
					// Continue loop to retry
				}
			} else {
				if status == "completed" {
					bp.MarkTaskComplete(task.SpaceKey) // Mark first
					// Add chunks using the document name as content after successful creation
					taskCompleted = true
					// Read updated progress for logging
					log.Printf("[SUCCESS] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					return
				} else if status == "deleted" {
					bp.MarkTaskComplete(task.SpaceKey) // Mark first
					taskCompleted = true
					log.Printf("[DELETED] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					return
				} else if status == "failed" { // Handle explicit failure status
					bp.MarkTaskComplete(task.SpaceKey) // Mark first
					taskCompleted = true
					log.Printf("[FAILED] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					// Optionally record this failure differently than 404
					return
				} else if status == "timeout" {
					if cfg.Concurrency.DeleteTimeoutContent {
						bp.MarkTaskComplete(task.SpaceKey) // Mark first
						taskCompleted = true
						log.Printf("[TIMEOUT] %s - %s.", bp.ProgressString(task.SpaceKey), logPrefix)
						return
					} else {
						task.Op.StartAt = time.Now() // Update start time for next check
						log.Printf("[TIMEOUT] %s - %s, indexing timeout, continue check.", bp.ProgressString(task.SpaceKey), logPrefix)
						continue
					}
				} else if status == "error" {
					task.Op.StartAt = time.Now() // Update start time for next check
					log.Printf("[ERROR] %s - %s, indexing error, continue check.", bp.ProgressString(task.SpaceKey), logPrefix)
					continue
				} else if status == "indexing" || status == "waiting" || status == "splitting" || status == "parsing" { // cleaning
					task.Op.StartAt = time.Now() // Update start time for next check
					continue
				} else {
					// Handle other statuses as needed
					// log.Printf("%s - %s Status: %s, continuing check.", progressStr, logPrefix, status)
					continue
				}
				// Else: status is "pending" or similar, continue loop
				//log.Printf("%s - %s Status: %s, continuing check.", progressStr, logPrefix, status)
			}

		case <-ctx.Done():
			if !taskCompleted { // Check if not already completed by status check
				log.Printf("[TIMEOUT] %s - %s Monitoring timed out, continue check.", bp.ProgressString(task.SpaceKey), logPrefix)
				continue
			}
			taskCompleted = true
			return

		case <-bp.shutdown:
			if !taskCompleted {
				// Task is aborted. Decide if aborted tasks should count towards completion.
				// Typically, they might not, so we might *not* call markTaskComplete here.
				// The overallWg is decremented anyway when the worker exits or handles the next task.
				log.Printf("[ABORTED] %s - %s Pool shutting down.", bp.ProgressString(task.SpaceKey), logPrefix)
			}
			return
		default:
			// No-op: Just keep waiting for the next tick or context cancellation
		}
	}
}

func (bp *BatchPool) MarkTaskComplete(spaceKey string) {
	val, ok := bp.progress.Load(spaceKey)
	if !ok {
		log.Printf("Error: Progress entry for spaceKey '%s' not found during completion.", spaceKey)
		// Consider creating a default entry on the fly if needed:
		// val, _ = bp.progress.LoadOrStore(spaceKey, &SpaceProgress{})
		return // Or handle more gracefully
	}
	sp := val.(*SpaceProgress)
	sp.completed.Add(1)
}

func (bp *BatchPool) SetTotal(spaceKey string, total int) {
	if total < 0 {
		total = 0
	}

	val, _ := bp.progress.LoadOrStore(spaceKey, &SpaceProgress{})
	sp := val.(*SpaceProgress)

	sp.total.Store(int32(total))
	sp.completed.Store(0) // Reset completed count

	bp.mu.Lock()
	totalStr := strconv.Itoa(total)
	if len(totalStr) > bp.totalLen {
		bp.totalLen = len(totalStr)
	}
	bp.mu.Unlock()
}

func (bp *BatchPool) ProgressString(spaceKey string) string {
	val, ok := bp.progress.Load(spaceKey)
	if !ok {
		return "?/?" // Space not initialized via SetTotal
	}
	sp := val.(*SpaceProgress)

	completed := sp.completed.Load()
	total := sp.total.Load()

	bp.mu.Lock() // Lock only to read totalLen
	tLen := bp.totalLen
	bp.mu.Unlock()

	// Use max length for alignment
	return fmt.Sprintf("%*d/%*d", tLen, completed, tLen, total)
}

func (bp *BatchPool) Wait() {
	bp.overallWg.Wait()
}

func (bp *BatchPool) Close() {
	bp.stopOnce.Do(func() {
		log.Println("Shutting down batch pool...")
		close(bp.shutdown)  // Signal workers to stop taking new tasks from queue
		close(bp.taskQueue) // Close task queue
	})

	// Wait for worker goroutines to finish processing any remaining tasks in the queue
	// and then exit their loops.
	bp.workersWg.Wait()

	log.Println("Batch pool shut down complete.")
}
