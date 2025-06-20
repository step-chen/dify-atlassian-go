package batchpool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config" // Import config package
)

type ContentType int

const (
	Page ContentType = iota
	Attachment
	Code
	GitFile
	LocalFile
)

// ActionType defines the type of operation to be performed.
type ActionType int8

const (
	ActionCreate   ActionType = 0  // Represents a create operation
	ActionUpdate   ActionType = 1  // Represents an update operation
	ActionDelete   ActionType = 2  // Represents a delete operation
	ActionNoAction ActionType = -1 // Represents no action
)

type Keywords map[int][]string

func (k *Keywords) Add(segOrderID int, keywordsToAdd []string) {
	if *k == nil {
		*k = make(Keywords)
	}
	if len(keywordsToAdd) == 0 {
		return
	}

	currentKeywords := (*k)[segOrderID]

	existingKeywordsMap := make(map[string]struct{})
	for _, kw := range currentKeywords {
		existingKeywordsMap[kw] = struct{}{}
	}

	for _, newKw := range keywordsToAdd {
		if _, exists := existingKeywordsMap[newKw]; !exists {
			currentKeywords = append(currentKeywords, newKw)
			existingKeywordsMap[newKw] = struct{}{} // Add to map to avoid duplicates within keywordsToAdd
		}
	}

	(*k)[segOrderID] = currentKeywords
}

func (k *Keywords) RemoveDuplicates() {
	if k == nil {
		return
	}

	for segOrderID, keywordList := range *k {
		if len(keywordList) == 0 {
			continue
		}

		seen := make(map[string]struct{})
		uniqueKeywords := make([]string, 0, len(keywordList))

		for _, keyword := range keywordList {
			if _, exists := seen[keyword]; !exists {
				seen[keyword] = struct{}{}
				uniqueKeywords = append(uniqueKeywords, keyword)
			}
		}

		(*k)[segOrderID] = uniqueKeywords
	}
}

type Operation struct {
	Action           ActionType  // 0: create, 1: update, 2: delete, -1: no action
	Type             ContentType // 0: page, 1: attachment
	LastModifiedDate string
	MediaType        string // Mime type
	DifyID           string
	//Keywords         Keywords
	StartAt time.Time
	Hash    string
}

type UpdateKeywordsFunc func(key, difyID string) (err error)
type StatusCheckerFunc func(ctx context.Context, key, id, title, batch string, op Operation) (int, string, Operation, error)

type Task struct {
	key   string
	id    string
	title string
	batch string
	op    Operation
}

type Progress struct {
	total     atomic.Int32 // Expected total tasks for this key
	completed atomic.Int32 // Completed tasks for this space
}

type BatchPool struct {
	maxWorkers     int
	statusChecker  StatusCheckerFunc
	updateKeywords UpdateKeywordsFunc
	taskQueue      chan Task      // Renamed from taskQueue to jobs for consistency if desired, but taskQueue is also clear
	workersWg      sync.WaitGroup // Waits for worker goroutines to finish on Close
	overallWg      sync.WaitGroup // Waits for all submitted tasks to complete processing
	stopOnce       sync.Once
	shutdown       chan struct{}
	progress       sync.Map // Stores map[string]*Progress for per-space tracking
	mu             sync.Mutex
	totalLen       int            // Max length of total count string across all spaces for formatting
	cfg            config.ConcCfg // Store only the concurrency config

	// Timeout tracking
	timeoutContents map[string]map[string]Operation // Stores map[spaceKey]map[contentID]Operation
	timeoutMutex    sync.Mutex                      // Mutex for protecting timeoutContents
}

func NewBatchPool(maxWorkers int, queueSize int, statusChecker StatusCheckerFunc, updateKeywords UpdateKeywordsFunc, concurrencyCfg config.ConcCfg) *BatchPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	if statusChecker == nil {
		log.Fatal("batchpool: statusChecker cannot be nil")
	}

	bp := &BatchPool{
		maxWorkers:     maxWorkers,
		statusChecker:  statusChecker,
		updateKeywords: updateKeywords,
		taskQueue:      make(chan Task, queueSize),
		shutdown:       make(chan struct{}),
		cfg:            concurrencyCfg, // Store concurrency config
		// progress is initialized implicitly by sync.Map
		timeoutContents: make(map[string]map[string]Operation),
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

func (bp *BatchPool) Add(ctx context.Context, key, id, title, batch, segEofTag string, op Operation) error {
	task := Task{
		key:   key,
		id:    id,
		title: title,
		batch: batch,
		op:    op,
	}

	if _, ok := bp.progress.Load(key); !ok {
		log.Printf("Warning: Adding task for key '%s' before SetTotal was called. Progress tracking might be inaccurate.", key)
		// Optionally create a default progress entry here if desired:
		// bp.progress.LoadOrStore(spaceKey, &Progress{})
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

// addTimeoutContent safely adds a task's operation to the timeoutContents map
func (bp *BatchPool) addTimeoutContent(task Task) {
	bp.timeoutMutex.Lock()
	defer bp.timeoutMutex.Unlock()

	if bp.timeoutContents[task.key] == nil {
		bp.timeoutContents[task.key] = make(map[string]Operation)
	}
	bp.timeoutContents[task.key][task.id] = task.op
}

func (bp *BatchPool) monitorBatch(task Task) {
	taskTimeout := time.Duration(bp.cfg.IndexingTimeout) * time.Minute
	if taskTimeout <= 0 {
		taskTimeout = 10 * time.Minute // Default timeout if config is invalid
	}
	ctx, cancel := context.WithTimeout(context.Background(), taskTimeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second) // Keep polling interval
	defer ticker.Stop()

	logPrefix := fmt.Sprintf("Key: %s, DifyID: %s, Title: %s, Batch: %s",
		task.key, task.op.DifyID, task.title, task.batch)

	taskCompleted := false // Flag to ensure completion logic runs only once

	defer func() {
		if !taskCompleted {
			bp.MarkTaskComplete(task.key)
			log.Printf("%s - Monitoring ended unexpectedly.", logPrefix)
		}
	}()

	for {
		select {
		case <-ticker.C:
			code, status, modifiedOp, err := bp.statusChecker(ctx, task.key, task.id, task.title, task.batch, task.op)
			progressStr := bp.ProgressString(task.key) // Get current progress string

			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					log.Printf("[CANCELLED] %s - %s Monitoring cancelled or timed out internally: %v", progressStr, logPrefix, err)
				} else {
					log.Printf("[ERROR] %s - %s Failed status check: %v. Retrying...", progressStr, logPrefix, err)
				}
			} else {
				task.op = modifiedOp // Update the task's operation with the one returned by statusChecker

				if status == "completed" {
					bp.MarkTaskComplete(task.key) // Mark first
					taskCompleted = true
					// Log success of the main task processing first
					log.Printf("[SUCCESS] %s - %s", bp.ProgressString(task.key), logPrefix)
					if bp.updateKeywords != nil && code == http.StatusOK {
						bp.updateKeywords(task.key, task.op.DifyID)
					}
					return
				} else if status == "deleted" {
					bp.MarkTaskComplete(task.key) // Mark first
					taskCompleted = true
					log.Printf("[DELETED] %s - %s", bp.ProgressString(task.key), logPrefix)
					bp.addTimeoutContent(task)
					return
				} else if status == "failed" { // Handle explicit failure status
					bp.MarkTaskComplete(task.key) // Mark first
					taskCompleted = true
					log.Printf("[FAILED] %s - %s", bp.ProgressString(task.key), logPrefix)
					// Optionally record this failure differently than 404
					return
				} else if status == "timeout" {
					if bp.cfg.DeleteTimeoutContent {
						bp.MarkTaskComplete(task.key) // Mark first
						taskCompleted = true
						log.Printf("[TIMEOUT] %s - %s.", bp.ProgressString(task.key), logPrefix)
						bp.addTimeoutContent(task)
						return
					} else {
						task.op.StartAt = time.Now() // Update start time for next check
						log.Printf("[TIMEOUT] %s - %s, indexing timeout, continue check.", bp.ProgressString(task.key), logPrefix)
						continue
					}
				} else if status == "error" {
					task.op.StartAt = time.Now() // Update start time for next check
					log.Printf("[ERROR] %s - %s, indexing error, continue check.", bp.ProgressString(task.key), logPrefix)
					continue
				} else if status == "indexing" || status == "waiting" || status == "splitting" || status == "parsing" { // cleaning
					task.op.StartAt = time.Now() // Update start time for next check
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
				log.Printf("[TIMEOUT] %s - %s Monitoring timed out, continue check.", bp.ProgressString(task.key), logPrefix)
			}
			return

		case <-bp.shutdown:
			if !taskCompleted {
				// Task is aborted. Decide if aborted tasks should count towards completion.
				// Typically, they might not, so we might *not* call markTaskComplete here.
				// The overallWg is decremented anyway when the worker exits or handles the next task.
				log.Printf("[ABORTED] %s - %s Pool shutting down.", bp.ProgressString(task.key), logPrefix)
			}
			return
		}
	}
}

func (bp *BatchPool) MarkTaskComplete(key string) {
	val, ok := bp.progress.Load(key)
	if !ok {
		log.Printf("Error: Progress entry for key '%s' not found during completion.", key)
		// Consider creating a default entry on the fly if needed:
		// val, _ = bp.progress.LoadOrStore(spaceKey, &Progress{})
		return // Or handle more gracefully
	}
	sp := val.(*Progress)
	sp.completed.Add(1)
}

func (bp *BatchPool) SetTotal(key string, total int) {
	if total < 0 {
		total = 0
	}

	val, _ := bp.progress.LoadOrStore(key, &Progress{})
	sp := val.(*Progress)

	sp.total.Store(int32(total))
	sp.completed.Store(0) // Reset completed count

	bp.mu.Lock()
	totalStr := strconv.Itoa(total)
	if len(totalStr) > bp.totalLen {
		bp.totalLen = len(totalStr)
	}
	bp.mu.Unlock()
}

func (bp *BatchPool) ProgressString(key string) string {
	val, ok := bp.progress.Load(key)
	if !ok {
		return "?/?" // Space not initialized via SetTotal
	}
	sp := val.(*Progress)

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

func (bp *BatchPool) HasTimeoutItems() bool {
	bp.timeoutMutex.Lock()
	defer bp.timeoutMutex.Unlock()
	return len(bp.timeoutContents) > 0
}

func (bp *BatchPool) ClearTimeoutContents() {
	bp.timeoutMutex.Lock()
	defer bp.timeoutMutex.Unlock()
	bp.timeoutContents = make(map[string]map[string]Operation)
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
