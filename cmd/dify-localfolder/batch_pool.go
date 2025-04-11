package main // Changed package to main as it's part of the cmd

import (
	"context"
	"log"
	"sync"
	"time"

	localfolder_cfg "github.com/step-chen/dify-atlassian-go/internal/config/localfolder" // Use the specific localfolder config package
)

// StatusCheckerFunc defines the function signature for checking batch status.
// It now uses datasetID, filePath, and FileOperation.
type StatusCheckerFunc func(ctx context.Context, datasetID, filePath, title, batch string, op FileOperation) (string, error)

// BatchTask represents a task to monitor a Dify batch process.
// Adapted for local files.
type BatchTask struct {
	DatasetID string             // Dify Dataset ID
	FilePath  string             // Original file path (used as unique ID within dataset)
	Title     string             // File title (e.g., filename)
	Batch     string             // Dify batch ID
	Operation FileOperation      // File operation details
	Retry     int                // Retry count
	Ctx       context.Context    // Context for this specific task, including potential timeout
	Cancel    context.CancelFunc // Function to cancel the task's context
}

// BatchPool manages a pool of workers to monitor Dify batch statuses concurrently.
type BatchPool struct {
	tasks       chan BatchTask
	wg          sync.WaitGroup
	maxWorkers  int
	checker     StatusCheckerFunc
	baseCfg     *localfolder_cfg.BaseConfig // Use the BaseConfig from the localfolder package
	mu          sync.Mutex
	activeTasks map[string]bool // Track active tasks by batch ID to prevent duplicates? Or by filePath? Let's use filePath.
	stopOnce    sync.Once
	stopCh      chan struct{} // Channel to signal workers to stop
}

// NewBatchPool creates a new BatchPool.
// It now takes the local folder specific StatusCheckerFunc and localfolder.BaseConfig.
func NewBatchPool(maxWorkers, queueSize int, checker StatusCheckerFunc, baseCfg *localfolder_cfg.BaseConfig) *BatchPool {
	if maxWorkers <= 0 {
		maxWorkers = 1 // Ensure at least one worker
	}
	if queueSize <= 0 {
		queueSize = 100 // Default queue size
	}
	pool := &BatchPool{
		tasks:       make(chan BatchTask, queueSize),
		maxWorkers:  maxWorkers,
		checker:     checker,
		baseCfg:     baseCfg,
		activeTasks: make(map[string]bool),
		stopCh:      make(chan struct{}),
	}

	pool.startWorkers()
	return pool
}

// startWorkers launches the worker goroutines.
func (p *BatchPool) startWorkers() {
	for i := 0; i < p.maxWorkers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker is the goroutine function that processes tasks from the channel.
func (p *BatchPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				// Channel closed, exit worker
				return
			}
			p.processTask(task)
		case <-p.stopCh:
			// Stop signal received, exit worker
			return
		}
	}
}

// processTask handles a single batch monitoring task.
func (p *BatchPool) processTask(task BatchTask) {
	defer task.Cancel() // Ensure context is cancelled when processing finishes or panics

	// Initial check immediately
	status, err := p.checker(task.Ctx, task.DatasetID, task.FilePath, task.Title, task.Batch, task.Operation)
	if err != nil {
		log.Printf("Error checking initial status for batch %s (File: %s): %v. Retrying...", task.Batch, task.FilePath, err)
		// Implement retry logic if needed, or just log and potentially mark as failed
	}

	// Polling loop
	ticker := time.NewTicker(10 * time.Second) // Poll every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-task.Ctx.Done(): // Check if task context was cancelled (e.g., timeout from checker or pool shutdown)
			log.Printf("Task context cancelled for batch %s (File: %s): %v", task.Batch, task.FilePath, task.Ctx.Err())
			p.markTaskDone(task.FilePath)
			return
		case <-p.stopCh: // Check if the pool is stopping
			log.Printf("Batch pool stopping, cancelling check for batch %s (File: %s)", task.Batch, task.FilePath)
			p.markTaskDone(task.FilePath)
			return
		case <-ticker.C:
			// Check status using the provided checker function
			status, err = p.checker(task.Ctx, task.DatasetID, task.FilePath, task.Title, task.Batch, task.Operation)
			if err != nil {
				log.Printf("Error checking status for batch %s (File: %s): %v", task.Batch, task.FilePath, err)
				// Consider retry logic or breaking the loop after too many errors
				// For now, just log and continue polling
				continue
			}

			// log.Printf("Status for batch %s (File: %s): %s", task.Batch, task.FilePath, status)

			if status == "completed" || status == "failed" || status == "deleted" { // Assuming 'deleted' is a final state from checker
				log.Printf("Batch %s (File: %s) reached final state: %s", task.Batch, task.FilePath, status)
				p.markTaskDone(task.FilePath)
				return // Exit loop for this task
			}
			// Continue polling if status is pending, indexing, splitting, cleaning, etc.
		}
	}
}

// Submit adds a new batch monitoring task to the pool.
// Uses local folder parameters.
func (p *BatchPool) Submit(datasetID, filePath, title, batch string, op FileOperation) {
	p.mu.Lock()
	// Check if a task for this file path is already active
	if _, exists := p.activeTasks[filePath]; exists {
		log.Printf("Task for file %s (Batch: %s) already submitted. Skipping.", filePath, batch)
		p.mu.Unlock()
		return
	}
	p.activeTasks[filePath] = true
	p.mu.Unlock()

	// Create a context with a timeout for the task itself (optional, checker might handle timeout)
	// If the checker handles timeout detection (like deleting and marking for retry),
	// we might not need a hard timeout here, but it can prevent orphaned tasks.
	// Timeout duration could be based on config (e.g., IndexingTimeout * MaxRetries * buffer)
	// taskTimeout := time.Duration(p.baseCfg.Concurrency.IndexingTimeout*(p.baseCfg.Concurrency.MaxRetries+1)+5) * time.Minute // Example timeout
	// ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)

	// Using context.Background() for now, checker handles timeout logic.
	ctx, cancel := context.WithCancel(context.Background())

	task := BatchTask{
		DatasetID: datasetID,
		FilePath:  filePath,
		Title:     title,
		Batch:     batch,
		Operation: op,
		Retry:     0, // Initial submission
		Ctx:       ctx,
		Cancel:    cancel,
	}

	select {
	case p.tasks <- task:
		// Task submitted successfully
	case <-p.stopCh:
		log.Printf("Batch pool stopped, cannot submit task for file %s (Batch: %s)", filePath, batch)
		cancel()                 // Cancel the context we created
		p.markTaskDone(filePath) // Ensure it's marked done if not submitted
	default:
		// Queue is full - this shouldn't happen with a buffered channel unless queueSize is tiny or producers are very fast
		log.Printf("Batch pool queue full. Dropping task for file %s (Batch: %s)", filePath, batch)
		cancel()                 // Cancel the context
		p.markTaskDone(filePath) // Mark as done since it was dropped
	}
}

// markTaskDone removes the task from the active list.
func (p *BatchPool) markTaskDone(filePath string) {
	p.mu.Lock()
	delete(p.activeTasks, filePath)
	p.mu.Unlock()
}

// Wait blocks until all submitted tasks have completed their processing loops.
func (p *BatchPool) Wait() {
	// This simple Wait checks if the queue is empty and workers might be idle.
	// A more robust Wait needs to track active processing tasks, not just submitted ones.
	// We can check the activeTasks map size.

	for {
		p.mu.Lock()
		activeCount := len(p.activeTasks)
		p.mu.Unlock()

		// log.Printf("Waiting for batch pool... Active tasks: %d", activeCount) // Debug logging

		if activeCount == 0 {
			// Additionally, check if the task channel is empty (might be needed if tasks finish quickly)
			// This is tricky because a task might be picked up right after the check.
			// Relying on activeTasks count is generally safer.
			break
		}
		time.Sleep(1 * time.Second) // Check periodically
	}
	// log.Println("Batch pool wait finished.") // Debug logging
}

// Close signals workers to stop and waits for them to finish.
func (p *BatchPool) Close() {
	p.stopOnce.Do(func() {
		log.Println("Closing batch pool...")
		close(p.stopCh) // Signal workers to stop by closing the channel

		// Wait for all worker goroutines to exit
		p.wg.Wait()

		// Close the tasks channel *after* workers have stopped
		close(p.tasks)
		log.Println("Batch pool closed.")
	})
}
