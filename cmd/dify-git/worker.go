package main

import (
	"log"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/git" // Assuming we might need git client methods directly later
)

// Job represents a task for the Git worker
type Job struct {
	Type       batchpool.ContentType // Type of job (e.g., Page for file)
	DocumentID string                // Dify document ID (for updates/deletes)
	RepoKey    string                // Identifier for the repository (e.g., workspace/repo_name)
	FilePath   string                // Relative path of the file within the repo
	FileHash   string                // Hash of the file content (optional, for change detection)
	Client     *dify.Client          // Dify client
	GitClient  *git.Client           // Git client (might not be needed directly in worker, but good to have)
	RepoPath   string                // Absolute path to the local cloned repository
	Op         batchpool.Operation   // Operation details (action, type, etc.)
}

// JobChannels holds the channel for distributing jobs
type JobChannels struct {
	Jobs chan Job
}

// worker processes jobs from the job channel
func worker(jobChan <-chan Job, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// Placeholder: Add logic to potentially wait if needed (e.g., rate limiting)
		// batchPool.WaitForAvailable() // Removed - BatchPool manages worker availability internally

		// For Git, we primarily deal with files, which we can map to 'Page' type
		// We might introduce other types later if needed (e.g., specific handling for large files)
		switch job.Type {
		case batchpool.Page: // Treat files as Pages
			switch job.Op.Action {
			case 0: // Create
				log.Printf("Worker received CREATE job for Repo: %s, File: %s", job.RepoKey, job.FilePath)
				if err := createDocument(&job); err != nil {
					log.Printf("Error processing create document job for %s/%s: %v", job.RepoKey, job.FilePath, err)
				}
			case 1: // Update
				if job.DocumentID == "" {
					// This case might happen if the initial sync failed but the file exists.
					// Or if the state tracking got out of sync. Attempt create.
					log.Printf("Warning: Update action requested but no DocumentID found for Repo: %s, File: %s. Attempting create.", job.RepoKey, job.FilePath)
					if err := createDocument(&job); err != nil {
						log.Printf("Error processing create-during-update document job for %s/%s: %v", job.RepoKey, job.FilePath, err)
					}
				} else {
					log.Printf("Worker received UPDATE job for Repo: %s, File: %s, DifyID: %s", job.RepoKey, job.FilePath, job.DocumentID)
					if err := updateDocument(&job); err != nil {
						log.Printf("Error processing update document job for %s/%s: %v", job.RepoKey, job.FilePath, err)
					}
				}
			case 2: // Delete
				if job.DocumentID == "" {
					// Should not happen if state tracking is correct, but log a warning.
					log.Printf("Warning: Delete action requested but no DocumentID found for Repo: %s, File: %s. Skipping deletion.", job.RepoKey, job.FilePath)
				} else {
					log.Printf("Worker received DELETE job for Repo: %s, File: %s, DifyID: %s", job.RepoKey, job.FilePath, job.DocumentID)
					if err := deleteDocument(&job); err != nil {
						log.Printf("Error processing delete document job for %s/%s: %v", job.RepoKey, job.FilePath, err)
					}
				}
			default:
				log.Printf("Unknown job action type for JobType Page (File): %d in Repo: %s, File: %s", job.Op.Action, job.RepoKey, job.FilePath)
			}
		// case batchpool.Attachment: // Add if handling non-page types becomes necessary
		// 	log.Printf("Attachment type not currently handled for Git.")
		default:
			log.Printf("Unknown job type: %v for Repo: %s, File: %s", job.Type, job.RepoKey, job.FilePath)
		}
	}
}
