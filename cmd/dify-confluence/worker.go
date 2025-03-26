package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// JobType defines the type of job to be processed
type JobType int

const (
	JobTypeContent JobType = iota
	JobTypeAttachment
	JobTypeDelete
)

// Job represents a unit of work for the worker
type Job struct {
	Type             JobType                // Type of job
	DocumentID       string                 // Document ID
	SpaceKey         string                 // Space key
	Content          *confluence.Content    // Content to be processed (optional)
	Attachment       *confluence.Attachment // Attachment to be processed (optional)
	Client           *dify.Client           // Dify client
	ConfluenceClient *confluence.Client     // Confluence client (optional)
	Op               confluence.ContentOperation
}

// JobChannels contains all job channels for workers
type JobChannels struct {
	Jobs chan Job
}

// worker processes jobs from the job channel
func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		batchPool.WaitForAvailable()
		switch job.Type {
		case JobTypeContent:
			if job.DocumentID == "" {
				if err := createDocument(&job); err != nil {
					log.Printf("error processing content job: %v", err)
				}
			} else {
				if err := updateDocument(&job); err != nil {
					log.Printf("error processing content job: %v", err)
				}
			}
		case JobTypeAttachment:
			if err := uploadDocumentByFile(&job); err != nil {
				log.Printf("error processing attachment job: %v", err)
			}
		case JobTypeDelete:
			if err := deleteDocument(&job); err != nil {
				log.Printf("error processing delete job: %v", err)
			}
		default:
			log.Printf("unknown job type: %v", job.Type)
		}
	}
}
