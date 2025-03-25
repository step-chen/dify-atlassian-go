package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// JobChannels contains all job channels for workers
type JobChannels struct {
	Content    chan jobContent
	Attachment chan jobAttachment
	Delete     chan jobDelete
}

// jobContent represents a unit of work for content processing
type jobContent struct {
	documentID string             // Document ID to be deleted
	spaceKey   string             // Space key for this job
	content    confluence.Content // Content to be processed
	client     *dify.Client       // Dify client for this job
	op         confluence.ContentOperation
}

// jobDelete represents a unit of work for document deletion
type jobDelete struct {
	documentID string       // Document ID to be deleted
	client     *dify.Client // Dify client for this job
}

// jobAttachment represents a unit of work for attachment upload
type jobAttachment struct {
	documentID       string                // Document ID to be deleted
	spaceKey         string                // Space key for this job
	attachment       confluence.Attachment // Attachment to be processed
	client           *dify.Client          // Dify client for this job
	confluenceClient *confluence.Client    // Confluence client for downloading attachments
	op               confluence.ContentOperation
}

// worker processes jobs from the job channel
func workerContent(jobChan <-chan jobContent, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobChan {
		batchPool.WaitForAvailable()
		if j.documentID == "" {
			if err := createDocument(&j); err != nil {
				log.Printf("Error processing job: %v", err)
			}
		} else {
			if err := updateDocument(&j); err != nil {
				log.Printf("Error processing job: %v", err)
			}
		}
	}
}

// attachmentWorker processes attachment upload jobs
func workerAttachment(jobChan <-chan jobAttachment, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobChan {
		batchPool.WaitForAvailable()
		if err := uploadDocumentByFile(&j); err != nil {
			log.Printf("Error processing attachment job: %v", err)
		}
	}
}

// deleteWorker processes document deletion jobs
func workerDelete(jobChan <-chan jobDelete, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range jobChan {
		batchPool.WaitForAvailable()
		if err := deleteDocument(&j); err != nil {
			log.Printf("Error processing delete job: %v", err)
		}
	}
}
