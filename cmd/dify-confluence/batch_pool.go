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
	maxWorkers       int
	statusChecker    func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error)
	taskQueue        chan Task
	workersWg        sync.WaitGroup // Waits for worker goroutines to finish on Close
	overallWg        sync.WaitGroup // Waits for all submitted tasks to complete processing
	stopOnce         sync.Once
	shutdown         chan struct{}
	progress         sync.Map // Stores map[string]*SpaceProgress for per-space tracking
	mu               sync.Mutex
	totalLen         int            // Max length of total count string across all spaces for formatting
	cfg              config.ConcCfg // Store only the concurrency config
	remainChunksTask map[string]Task
	chunksWg         sync.WaitGroup // Waits for remain chunks task processing to finish
}

func NewBatchPool(maxWorkers int, queueSize int, statusChecker func(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) (string, error), concurrencyCfg config.ConcCfg) *BatchPool { // Accept ConcCfg directly
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}

	bp := &BatchPool{
		maxWorkers:       maxWorkers,
		statusChecker:    statusChecker,
		taskQueue:        make(chan Task, queueSize),
		shutdown:         make(chan struct{}),
		cfg:              concurrencyCfg,        // Store concurrency config
		remainChunksTask: make(map[string]Task), // Initialize the map
		// progress is initialized implicitly by sync.Map
	}

	bp.workersWg.Add(maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go bp.worker()
	}

	go bp.RunRemainChunksTask()

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

func (bp *BatchPool) Add(ctx context.Context, spaceKey, confluenceID, title, batch string, op confluence.ContentOperation) error {
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

func (bp *BatchPool) addChunks(task Task) error {
	if c, ok := difyClients[task.SpaceKey]; ok {
		if err := c.AddChunks(task.Op.DifyID, task.Title); err != nil {
			return fmt.Errorf("failed to add chunks for DifyID %s: %w", task.Op.DifyID, err)
		}
	} else {
		return fmt.Errorf("no Dify client for space %s", task.SpaceKey)
	}
	return nil
}

func (bp *BatchPool) monitorBatch(task Task) {
	taskTimeout := time.Duration(bp.cfg.IndexingTimeout) * time.Minute // Access IndexingTimeout directly
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()

	ticker := time.NewTicker(20 * time.Second) // Keep polling interval
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
					if err = bp.addChunks(task); err != nil {
						bp.addChunksTask(task) // Add to remainChunksTask for retry
						log.Printf("failed to add title chunk for document %s: %s", task.Title, err.Error())
					}
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
					bp.addChunksTask(task)             // Add to remainChunksTask for retry
					taskCompleted = true
					log.Printf("[FAILED] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					// Optionally record this failure differently than 404
					return
				} else if status == "timeout" {
					bp.MarkTaskComplete(task.SpaceKey) // Mark first

					if err = bp.addChunks(task); err != nil {
						bp.addChunksTask(task) // Add to remainChunksTask for retry
						log.Printf("failed to add title chunk for document %s: %s", task.Title, err.Error())
					}
					taskCompleted = true
					log.Printf("[TIMEOUT] %s - %s", bp.ProgressString(task.SpaceKey), logPrefix)
					// Optionally record this timeout differently than 404
					return
				} else {
					// Handle other statuses as needed
					continue
				}
				// Else: status is "pending" or similar, continue loop
				//log.Printf("%s - %s Status: %s, continuing check.", progressStr, logPrefix, status)
			}

		case <-ctx.Done():
			if !taskCompleted { // Check if not already completed by status check
				bp.MarkTaskComplete(task.SpaceKey)
				bp.addChunksTask(task) // Add to remainChunksTask for retry
				taskCompleted = true
				log.Printf("[TIMEOUT] %s - %s Monitoring timed out.", bp.ProgressString(task.SpaceKey), logPrefix)
			}
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

	// Wait for remain chunks task processing to finish
	bp.chunksWg.Wait()
	log.Println("Batch pool shut down complete.")
}

// AddChunksTask adds a task to remainChunksTask map
func (bp *BatchPool) addChunksTask(task Task) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.remainChunksTask[task.Op.DifyID] = task
}

// DelChunksTask removes a task from remainChunksTask map
func (bp *BatchPool) delChunksTask(difyID string) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	delete(bp.remainChunksTask, difyID)
}

func (bp *BatchPool) RunRemainChunksTask() {
	bp.chunksWg.Add(1)       // Correctly increments the WaitGroup for this goroutine
	defer bp.chunksWg.Done() // Ensures WaitGroup is decremented on exit

	ticker := time.NewTicker(time.Minute) // Retries every minute
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Periodic retry trigger
			bp.mu.Lock() // Lock to safely access remainChunksTask
			// Create a copy of tasks to process outside the lock
			tasks := make([]Task, 0, len(bp.remainChunksTask))
			for _, task := range bp.remainChunksTask {
				tasks = append(tasks, task)
			}
			bp.mu.Unlock() // Unlock quickly

			processedCount := 0   // Track successful retries in this tick
			failedCount := 0      // Track failed retries in this tick
			currentRemaining := 0 // Track remaining after this tick's processing attempt

			for _, task := range tasks {
				// Attempt to add chunks again
				if err := bp.addChunks(task); err == nil {
					// Success: Remove from the map
					// Double-check if it still exists before deleting,
					// although unlikely to be removed elsewhere concurrently.
					if _, exists := bp.remainChunksTask[task.Op.DifyID]; exists {
						bp.delChunksTask(task.Op.DifyID) // Remove from the map
						processedCount++
					}
				} else {
					// Failure: Log the error and keep it for the next retry
					log.Printf("Retry AddChunks failed for DifyID %s (Title: %s): %v", task.Op.DifyID, task.Title, err)
					failedCount++
				}
			}

			// Get the count remaining *after* processing this batch
			bp.mu.Lock()
			currentRemaining = len(bp.remainChunksTask)
			bp.mu.Unlock()

			// Log summary only if there were tasks to process
			if len(tasks) > 0 {
				log.Printf("Processed remaining chunks tasks: %d successful, %d failed, %d remaining.", processedCount, failedCount, currentRemaining)
			}

		case <-bp.shutdown: // Shutdown signal received
			log.Println("RunRemainChunksTask: Shutdown signal received. Processing final remaining tasks...")
			// Process all remaining tasks one last time before shutting down.
			// Lock for the entire final processing duration to prevent race conditions
			// where a task might be added by monitorBatch after the initial read
			// but before this goroutine exits.
			bp.mu.Lock() // Lock before reading and processing
			tasks := make([]Task, 0, len(bp.remainChunksTask))
			for _, task := range bp.remainChunksTask {
				tasks = append(tasks, task)
			}
			initialCount := len(tasks)
			processedCount := 0
			failedCount := 0

			for _, task := range tasks {
				if err := bp.addChunks(task); err == nil {
					// Still need to delete from the map even during shutdown processing
					delete(bp.remainChunksTask, task.Op.DifyID)
					processedCount++
				} else {
					// Log final failures
					log.Printf("Final AddChunks attempt failed during shutdown for DifyID %s (Title: %s): %v", task.Op.DifyID, task.Title, err)
					failedCount++
				}
			}
			finalRemaining := len(bp.remainChunksTask) // Get final count under lock
			bp.mu.Unlock()                             // Unlock after final processing

			log.Printf("Processed final %d remaining chunks tasks before shutdown: %d successful, %d failed, %d still remaining.", initialCount, processedCount, failedCount, finalRemaining)
			return // Exit the goroutine
		}
	}
}
