package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// Job represents a directory file processing job
type Job struct {
	Type              batchpool.ContentType // Type of job
	Keywords          []string              // Extracted keywords from content
	DocumentID        string                // Document ID
	RootDir           string
	RelativePath      string       // File path
	Content           string       // File content
	Client            *dify.Client // Dify client
	Op                batchpool.Operation
	DirKey            string // Directory key (dir1, dir2)
	PreprocessingFile string
}

type JobChannels struct {
	Jobs chan Job
}

func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// batchPool.WaitForAvailable() // Removed - BatchPool manages worker availability internally
		switch job.Type {
		case batchpool.LocalFile:
			switch job.Op.Action {
			case batchpool.ActionCreate:
				if err := createDocument(&job); err != nil {
					log.Printf("error processing create content job: %v", err)
				}
			case batchpool.ActionUpdate:
				if job.DocumentID == "" {
					log.Printf("Warning: Update action requested but no DocumentID found for file %s. Attempting create.", job.RelativePath)
					if err := createDocument(&job); err != nil {
						log.Printf("error processing create-during-update content job: %v", err)
					}
				} else {
					if err := updateDocument(&job); err != nil {
						log.Printf("error processing update content job: %v", err)
					}
				}
			case batchpool.ActionDelete:
				if job.DocumentID == "" {
					log.Printf("Warning: Delete action requested but no DocumentID found for file %s. Skipping deletion.", job.RelativePath)
				} else {
					if err := deleteDocument(&job); err != nil {
						log.Printf("error processing delete content job: %v", err)
					}
				}
			default:
				log.Printf("unknown job action type %d for LocalFile: %s", job.Op.Action, job.RelativePath)
			}
		default:
			log.Printf("unknown job type: %v", job.Type)
		}
	}
}
