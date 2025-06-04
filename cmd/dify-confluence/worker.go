package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

type Job struct {
	Type             batchpool.ContentType // Type of job
	DocumentID       string                // Document ID
	SpaceKey         string                // Space key
	Content          *confluence.Content   // Content to be processed (optional)
	Client           *dify.Client          // Dify client
	ConfluenceClient *confluence.Client    // Confluence client (optional)
	Op               batchpool.Operation
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
			case batchpool.ActionCreate:
				if err := createDocument(&job); err != nil {
					log.Printf("error processing create content job: %v", err)
				}
			case batchpool.ActionUpdate:
				if job.DocumentID == "" {
					log.Printf("Warning: Update action requested but no DocumentID found for Confluence ID %s. Attempting create.", job.Content.ID)
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
					log.Printf("Warning: Delete action requested but no DocumentID found for Confluence ID %s. Skipping deletion.", job.Content.ID)
				} else {
					if err := deleteDocument(&job); err != nil {
						log.Printf("error processing delete content job: %v", err)
					}
				}
			default:
				log.Printf("unknown job action type %d for Page/Attachment", job.Op.Action)
			}
		default:
			log.Printf("unknown job type: %v", job.Type)
		}
	}
}
