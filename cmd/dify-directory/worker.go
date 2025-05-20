package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// Job represents a directory file processing job
type Job struct {
	Type       batchpool.ContentType // Type of job
	DocumentID string                // Document ID
	FilePath   string                // File path
	Content    string                // File content
	Client     *dify.Client          // Dify client
	Op         batchpool.Operation
	DirKey     string // Directory key (dir1, dir2)
}

type JobChannels struct {
	Jobs chan Job
}

func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// batchPool.WaitForAvailable() // Removed - BatchPool manages worker availability internally
		switch job.Type {
		case batchpool.Page, batchpool.Attachment:
			switch job.Op.Action {
			case 0: // Create
				if err := createDocument(&job); err != nil {
					log.Printf("error processing create content job: %v", err)
				}
			case 1: // Update (or Create if ID missing - though initOperations should handle this)
				if job.DocumentID == "" {
					log.Printf("Warning: Update action requested but no DocumentID found for file %s. Attempting create.", job.FilePath)
					if err := createDocument(&job); err != nil {
						log.Printf("error processing create-during-update content job: %v", err)
					}
				} else {
					if err := updateDocument(&job); err != nil {
						log.Printf("error processing update content job: %v", err)
					}
				}
			case 2: // Delete
				if job.DocumentID == "" {
					log.Printf("Warning: Delete action requested but no DocumentID found for file %s. Skipping deletion.", job.FilePath)
				} else {
					if err := deleteDocument(&job); err != nil {
						log.Printf("error processing delete content job: %v", err)
					}
				}
			default:
				log.Printf("unknown job action type for JobTypeContent: %d", job.Op.Action)
			}
		default:
			log.Printf("unknown job type: %v", job.Type)
		}
	}
}
