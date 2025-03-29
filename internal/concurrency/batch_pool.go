package concurrency

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

// Task represents a batch monitoring task (same as before)
type Task struct {
	SpaceKey     string
	ConfluenceID string
	Title        string
	Batch        string
	Op           confluence.ContentOperation
}

// NotFoundErrorInfo stores details of 404 errors (datasetID added, keep if needed)
type NotFoundErrorInfo struct {
	DatasetID    string // Added field
	SpaceKey     string // Confluence space key
	ConfluenceID string // Confluence document ID
	Title        string // Document title
}

// SpaceProgress stores atomic progress counters for a specific spaceKey
type SpaceProgress struct {
	total     atomic.Int64 // Expected total tasks for this space
	completed atomic.Int64 // Completed tasks for this space
}

// BatchPool manages concurrent batch processing using a worker pool pattern
type BatchPool struct {
	maxWorkers     int
	statusChecker  func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error)
	taskQueue      chan Task
	workersWg      sync.WaitGroup // Waits for worker goroutines to finish on Close
	overallWg      sync.WaitGroup // Waits for all submitted tasks to complete processing
	stopOnce       sync.Once
	shutdown       chan struct{}
	progress       sync.Map // Stores map[string]*SpaceProgress for per-space tracking
	mu             sync.Mutex
	notFoundErrors map[string]NotFoundErrorInfo
	totalLen       int            // Max length of total count string across all spaces for formatting
	cfg            *config.Config // Add config field
}

// NewBatchPool creates a new batch processing pool
func NewBatchPool(maxWorkers int, queueSize int, statusChecker func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error), cfg *config.Config) *BatchPool { // Add cfg parameter
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}

	bp := &BatchPool{
		maxWorkers:     maxWorkers,
		statusChecker:  statusChecker,
		taskQueue:      make(chan Task, queueSize),
		shutdown:       make(chan struct{}),
		notFoundErrors: make(map[string]NotFoundErrorInfo),
		cfg:            cfg, // Store config
		// progress is initialized implicitly by sync.Map
	}

	bp.workersWg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go bp.worker()
	}

	return bp
}

// worker runs in a goroutine, processing tasks from the taskQueue
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

// Add submits a new batch task to the pool for monitoring
// IMPORTANT: Call SetTotal for the relevant spaceKey *before* adding tasks for it.
func (bp *BatchPool) Add(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) error {
	task := Task{
		SpaceKey:     spaceKey,
		ConfluenceID: confluenceID,
		Title:        title,
		Batch:        batch,
		Op:           op,
	}

	// Ensure progress entry exists (SetTotal should have been called)
	if _, ok := bp.progress.Load(spaceKey); !ok {
		// Log or handle this strictly? For now, log a warning.
		// Consider returning an error if SetTotal is mandatory before Add.
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

// monitorBatch performs the actual status checking for a task
func (bp *BatchPool) monitorBatch(task Task) {
	// Use timeout from config
	taskTimeout := time.Duration(bp.cfg.Concurrency.IndexingTimeout) * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second) // Keep polling interval
	defer ticker.Stop()

	logPrefix := fmt.Sprintf("Space: %s, DifyID: %s, Title: %s, Batch: %s",
		task.SpaceKey, task.Op.DifyID, task.Title, task.Batch)

	taskCompleted := false // Flag to ensure completion logic runs only once

	defer func() {
		// Ensure completion is marked even if panic or unexpected exit occurs
		// Although the main paths should handle this.
		if !taskCompleted {
			bp.markTaskComplete(task.SpaceKey)
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
				} else if err.Error() == "unexpected status code: 404" {
					log.Printf("[NOT FOUND] %s - %s", progressStr, logPrefix)
					bp.recordNotFoundError(task.Op.DifyID, task.SpaceKey, task.ConfluenceID, task.Title, task.Op.DatasetID) // Pass datasetID if needed
					bp.markTaskComplete(task.SpaceKey)
					taskCompleted = true
					return
				} else {
					log.Printf("[ERROR] %s - %s Failed status check: %v. Retrying...", progressStr, logPrefix, err)
					// Continue loop to retry
				}
			} else {
				if status == "completed" {
					bp.markTaskComplete(task.SpaceKey) // Mark first
					taskCompleted = true
					// Read updated progress for logging
					log.Printf("[SUCCESS] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					return
				} else if status == "deleted" {
					bp.markTaskComplete(task.SpaceKey) // Mark first
					taskCompleted = true
					log.Printf("[DELETED] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					return
				} else if status == "failed" { // Handle explicit failure status
					bp.markTaskComplete(task.SpaceKey) // Mark first
					taskCompleted = true
					log.Printf("[FAILED] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					// Optionally record this failure differently than 404
					return
				}
				// Else: status is "pending" or similar, continue loop
				//log.Printf("%s - %s Status: %s, continuing check.", progressStr, logPrefix, status)
			}

		case <-ctx.Done():
			// Task monitoring timeout or context cancelled externally
			if !taskCompleted { // Check if not already completed by status check
				bp.markTaskComplete(task.SpaceKey)
				taskCompleted = true
				log.Printf("[TIMEOUT] %s - %s Monitoring timed out.", bp.ProgressString(task.SpaceKey), logPrefix)
			}
			return

		case <-bp.shutdown:
			// Pool is shutting down globally
			if !taskCompleted {
				// Task is aborted. Decide if aborted tasks should count towards completion.
				// Typically, they might not, so we might *not* call markTaskComplete here.
				// The overallWg is decremented anyway when the worker exits or handles the next task.
				log.Printf("[ABORTED] %s - %s Pool shutting down.", bp.ProgressString(task.SpaceKey), logPrefix)
			}
			return
		}
	}
}

// markTaskComplete atomically increments the completed count for the spaceKey.
func (bp *BatchPool) markTaskComplete(spaceKey string) {
	val, ok := bp.progress.Load(spaceKey)
	if !ok {
		// This case should ideally not happen if SetTotal was called.
		log.Printf("Error: Progress entry for spaceKey '%s' not found during completion.", spaceKey)
		// Consider creating a default entry on the fly if needed:
		// val, _ = bp.progress.LoadOrStore(spaceKey, &SpaceProgress{})
		return // Or handle more gracefully
	}
	sp := val.(*SpaceProgress)
	sp.completed.Add(1)
}

// SetTotal configures the expected total operations count for a given spaceKey.
// Call this *before* adding tasks for the spaceKey for accurate progress tracking.
func (bp *BatchPool) SetTotal(spaceKey string, total int) {
	if total < 0 {
		total = 0
	}

	// Get or create the progress tracker for this space
	val, _ := bp.progress.LoadOrStore(spaceKey, &SpaceProgress{})
	sp := val.(*SpaceProgress)

	// Store the total atomically
	sp.total.Store(int64(total))

	// Update max total length for formatting (protected by mutex)
	bp.mu.Lock()
	totalStr := strconv.Itoa(total)
	if len(totalStr) > bp.totalLen {
		bp.totalLen = len(totalStr)
	}
	bp.mu.Unlock()
}

// ProgressString formats the progress for a specific spaceKey.
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

// recordNotFoundError logs 404 error details thread-safely
func (bp *BatchPool) recordNotFoundError(difyID, spaceKey, confluenceID, title, datasetID string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.notFoundErrors[difyID] = NotFoundErrorInfo{
		DatasetID:    datasetID, // Store datasetID
		SpaceKey:     spaceKey,
		ConfluenceID: confluenceID,
		Title:        title,
	}
}

// LogNotFoundErrors outputs all recorded 404 errors thread-safely
func (bp *BatchPool) LogNotFoundErrors() {
	bp.mu.Lock()
	defer bp.mu.Unlock() // Ensure unlock happens even if logging panics

	if len(bp.notFoundErrors) == 0 {
		log.Println("No '404 Not Found' errors were recorded during execution.")
		return
	}

	log.Println("--- Documents Not Found (404 Errors) ---")
	for difyID, info := range bp.notFoundErrors {
		log.Printf("DifyID: %s, DatasetID: %s, SpaceKey: %s, ConfluenceID: %s, Title: %s",
			difyID, info.DatasetID, info.SpaceKey, info.ConfluenceID, info.Title)
	}
	log.Println("----------------------------------------")
}

// Wait blocks until all submitted tasks have been processed by the workers.
// Call this after adding all tasks.
func (bp *BatchPool) Wait() {
	bp.overallWg.Wait()
}

// Close gracefully shuts down the batch pool.
// Stops accepting new tasks and waits for current tasks and workers to finish.
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
