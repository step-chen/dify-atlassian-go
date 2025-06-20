package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/bitbucket"
	CFG "github.com/step-chen/dify-atlassian-go/internal/config/bitbucket"
	"github.com/step-chen/dify-atlassian-go/internal/content_parser"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// Job defines a task for a worker to process.
type Job struct {
	Type            batchpool.ContentType // Type of job
	BatchPool       *batchpool.BatchPool
	DocumentID      string             // Document ID
	Key             string             // Key
	Content         *bitbucket.Content // Content to be processed (optional)
	Client          *dify.Client       // Dify client
	BitbucketClient *bitbucket.Client  // Confluence client (optional)
	Op              batchpool.Operation
	Parser          *content_parser.IParser
	RepoCFG         *CFG.RepoInfo
}

// JobChannels holds channels for job distribution.
type JobChannels struct {
	Jobs chan Job
}

func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// batchPool.WaitForAvailable() // Removed - BatchPool manages worker availability internally
		switch job.Type {
		case batchpool.Page, batchpool.Attachment, batchpool.GitFile:
			switch job.Op.Action {
			case batchpool.ActionCreate:
				if err := createDocument(&job); err != nil {
					log.Printf("error processing create content job: %v", err)
				}
			case batchpool.ActionUpdate:
				if job.DocumentID == "" {
					log.Printf("Warning: Update action requested but no DocumentID found for Bitbucket ID %s. Attempting create.", job.Content.ID)
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
					log.Printf("Warning: Delete action requested but no DocumentID found for Bitbucket ID %s. Skipping deletion.", job.Content.ID)
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
