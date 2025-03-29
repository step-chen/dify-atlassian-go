package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
)

// JobType enumerates possible job types
// JobTypeContent: Process Confluence content
// JobTypeAttachment: Process Confluence attachment
// JobTypeDelete: Delete document
type JobType int

const (
	JobTypeContent JobType = iota
	JobTypeAttachment
	JobTypeDelete
)

// Job contains all information needed to process a work unit
// Type: Job type (content/attachment/delete)
// DocumentID: Target document ID
// SpaceKey: Confluence space key
// Content: Confluence content data (optional)
// Attachment: Confluence attachment data (optional)
// Client: Dify API client
// ConfluenceClient: Confluence API client (optional)
// Op: Content operation details
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

// JobChannels manages job distribution channels
// Jobs: Channel for sending jobs to workers
type JobChannels struct {
	Jobs chan Job
}

// worker processes jobs from the job channel
// jobChan: Channel to receive jobs from
// wg: WaitGroup to signal job completion
// Handles content creation/update, attachment upload, and document deletion
func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// batchPool.WaitForAvailable() // Removed - BatchPool manages worker availability internally
		switch job.Type {
		case JobTypeContent:
			// Determine action based on job.Op.Action
			// Action 0: Create, Action 1: Update (or Create if DocumentID is empty), Action 2: Delete
			switch job.Op.Action {
			case 0: // Create
				if err := createDocument(&job); err != nil {
					log.Printf("error processing create content job: %v", err)
					// Consider how to handle create errors - maybe mark in batch pool?
				}
			case 1: // Update (or Create if ID missing - though initOperations should handle this)
				if job.DocumentID == "" {
					// Use job.Content.ID for logging the Confluence ID
					log.Printf("Warning: Update action requested but no DocumentID found for Confluence ID %s. Attempting create.", job.Content.ID)
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
					// Use job.Content.ID for logging the Confluence ID
					log.Printf("Warning: Delete action requested but no DocumentID found for Confluence ID %s. Skipping deletion.", job.Content.ID)
					// If deletion is crucial even without DifyID, need alternative logic.
					// Since deletion doesn't go to batch pool, we might need to mark completion differently.
					// For now, skip if no DifyID.
				} else {
					if err := deleteDocument(&job); err != nil {
						log.Printf("error processing delete content job: %v", err)
					}
				}
			default:
				log.Printf("unknown job action type for JobTypeContent: %d", job.Op.Action)
			}
		case JobTypeAttachment:
			// Attachments are typically created/uploaded, maybe updated?
			// Assuming Action 0/1 mean upload/update, Action 2 means delete associated Dify doc.
			switch job.Op.Action {
			case 0, 1: // Create or Update attachment -> Upload/Re-upload
				if err := uploadDocumentByFile(&job); err != nil {
					log.Printf("error processing upload attachment job: %v", err)
				}
			case 2: // Delete attachment
				if job.DocumentID == "" {
					// Use job.Attachment.ID for logging the Confluence ID
					log.Printf("Warning: Delete action requested for attachment but no DocumentID found for Confluence ID %s. Skipping deletion.", job.Attachment.ID)
				} else {
					if err := deleteDocument(&job); err != nil {
						log.Printf("error processing delete attachment job: %v", err)
					}
				}
			default:
				log.Printf("unknown job action type for JobTypeAttachment: %d", job.Op.Action)
			}
		default:
			log.Printf("unknown job type: %v", job.Type)
		}
	}
}
