package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	git_cfg "github.com/step-chen/dify-atlassian-go/internal/config/git" // Use alias for git config
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	"github.com/step-chen/dify-atlassian-go/internal/git"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

var (
	// Map repoKey (e.g., "workspace/repo") to Dify client
	difyClients map[string]*dify.Client
	batchPool   *batchpool.BatchPool
	cfg         *git_cfg.Config // Use the specific Git config type
	gitClient   *git.Client
)

// repoKey generates a unique key for a repository based on workspace and repo name.
func repoKey(workspace, repo string) string {
	return fmt.Sprintf("%s/%s", workspace, repo)
}

func main() {
	configFile := flag.String("c", "config.yaml", "Path to the configuration file")
	flag.Parse()

	var err error
	cfg, err = git_cfg.LoadConfig(*configFile) // Use Git config loader
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", *configFile, err)
	}

	utils.InitRequiredTools(utils.ToolGit) // Ensure necessary tools (like git) are available

	// --- Initialize Git Client ---
	gitClient, err = git.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Git client: %v", err)
	}

	// --- Initialize Batch Pool ---
	// Status checker needs to map repoKey back to the correct Dify client
	batchPool = batchpool.NewBatchPool(
		cfg.Concurrency.BatchPoolSize,
		cfg.Concurrency.QueueSize,
		func(ctx context.Context, key, id, title, batch string, op batchpool.Operation) (int, string, batchpool.Operation, error) {
			if c, ok := difyClients[key]; !ok {
				err := fmt.Errorf("dify client not found for key %s in statusChecker", key)
				log.Println(err.Error())
				return -1, "", op, err
			} else {
				return c.CheckBatchStatus(ctx, key, id, title, batch, "code", op, cfg.Concurrency.IndexingTimeout, cfg.Concurrency.DeleteTimeoutContent)
			}
		},
		nil,
		cfg.Concurrency,
	)

	// --- Initialize Dify Clients ---
	difyClients = make(map[string]*dify.Client)
	for workspace, wsConfig := range cfg.Git.Workspaces {
		for _, repoName := range wsConfig.Repositories {
			key := repoKey(workspace, repoName)
			datasetID, exists := cfg.Dify.Datasets[key] // Expect mapping like "workspace/repo": "dataset_id"
			if !exists || datasetID == "" {
				log.Fatalf("Dify dataset mapping missing or empty for repository: %s", key)
			}

			client, err := dify.NewClient(cfg.Dify.BaseURL, cfg.Dify.APIKey, datasetID, "", cfg, false) // Pass cfg as provider
			if err != nil {
				log.Fatalf("Failed to create Dify client for repo %s (dataset %s): %v", key, datasetID, err)
			}
			// Initialize metadata (fetches Dify fields definition)
			if err = client.InitMetadata(); err != nil {
				log.Fatalf("Failed to initialize Dify metadata for repo %s: %v", key, err)
			}
			difyClients[key] = client
			log.Printf("Initialized Dify client for repo %s -> dataset %s", key, datasetID)
		}
	}

	// --- Start Workers ---
	jobChannels := JobChannels{
		Jobs: make(chan Job, cfg.Concurrency.QueueSize),
	}
	var wg sync.WaitGroup
	log.Printf("Starting %d workers...", cfg.Concurrency.Workers)
	for i := 0; i < cfg.Concurrency.Workers; i++ {
		wg.Add(1)
		// Pass gitClient to the worker if needed, although document.go handles most git interactions now
		go worker(jobChannels.Jobs, &wg)
	}

	// --- Main Processing Loop ---
	runProcessingLoop(&jobChannels) // Pass job channel

	// --- Wait and Cleanup ---
	log.Println("Closing job channel...")
	close(jobChannels.Jobs)

	log.Println("Waiting for all workers to complete...")
	wg.Wait()
	log.Println("All workers finished.")

	// Wait for batch pool monitoring tasks (important!)
	log.Println("Waiting for batch pool monitoring tasks to complete...")
	batchPool.Wait()
	log.Println("Batch pool monitoring complete.")

	batchPool.Close() // Close batch pool after workers and monitoring are done

	log.Println("Processing complete.")
	// utils.WriteFailedTypesLog() // Keep if relevant for Git processing errors
}

// runProcessingLoop handles cloning/pulling repos and processing each one.
func runProcessingLoop(jobChannels *JobChannels) {
	log.Println("Cloning/Pulling repositories...")
	if err := gitClient.CloneRepositories(); err != nil {
		log.Fatalf("Failed to clone/pull repositories: %v", err)
	}
	log.Println("Repositories updated.")

	// Process repositories (initial run)
	// Retries might be handled differently or simplified for Git compared to Confluence timeouts
	processAllRepositories(jobChannels)

	// TODO: Implement retry logic if needed, similar to Confluence main,
	// based on batchPool.HasTimeoutItems() after batchPool.Wait().
	// For now, we do a single pass.
}

// processAllRepositories iterates through configured repos and calls processRepository.
func processAllRepositories(jobChannels *JobChannels) {
	totalFilesProcessed := 0
	totalJobsCreated := 0 // Track jobs created in this run

	for workspace, wsConfig := range cfg.Git.Workspaces {
		for _, repoName := range wsConfig.Repositories {
			key := repoKey(workspace, repoName)
			difyClient, dcExists := difyClients[key]
			if !dcExists {
				log.Printf("Warning: Dify client not found for repo %s during processing. Skipping.", key)
				continue
			}

			repoLocalPath := filepath.Join(cfg.Git.TargetDir, workspace, repoName)
			log.Printf("Processing repository: %s (Path: %s)", key, repoLocalPath)

			// Fetch existing Dify metadata for this dataset
			// Use GetAllDocumentsMetadata which is designed for local file sync
			log.Printf("Fetching existing Dify metadata for %s...", difyClient.BaseURL())
			difyMetadataMap, err := difyClient.GetAllDocumentsMetadata()
			if err != nil {
				log.Printf("Error fetching Dify metadata for repo %s: %v. Skipping repo.", key, err)
				continue // Skip this repo if we can't get metadata
			}
			log.Printf("Found %d existing metadata records in Dify for %s.", len(difyMetadataMap), difyClient.BaseURL())

			// Get local file list and basic info
			localFileOps, err := gitClient.ProcessRepository(workspace, repoName)
			if err != nil {
				log.Printf("Error processing local repository %s: %v. Skipping repo.", key, err)
				continue
			}
			log.Printf("Found %d potentially relevant local files in %s.", len(localFileOps), repoLocalPath)
			batchPool.SetTotal(key, len(localFileOps)) // Set total for progress tracking

			// --- Compare and Generate Jobs ---
			processedLocalFiles := make(map[string]bool) // Track processed local files to find deletions

			// 1. Iterate through local files: Check for Creates or Updates
			for localRelPath, localOp := range localFileOps {
				processedLocalFiles[localRelPath] = true
				localFullPath := filepath.Join(repoLocalPath, localRelPath)
				docID := generateDocID(key, localRelPath) // Generate consistent ID

				// Calculate local hash (only if needed for comparison)
				localHash := "" // Initialize empty

				existingMeta, metaExists := difyMetadataMap[docID]

				action := batchpool.ActionNoAction // Use ActionType constants

				if !metaExists {
					action = batchpool.ActionCreate // Create if not in Dify metadata
					log.Printf("Detected CREATE for %s", localRelPath)
				} else {
					// Exists in Dify, check if update needed (compare hash)
					// Calculate hash only if it exists in Dify to compare
					contentBytes, err := os.ReadFile(localFullPath)
					if err != nil {
						log.Printf("Error reading local file %s for hash comparison: %v. Skipping update check.", localFullPath, err)
						continue // Skip this file if unreadable
					}
					localHash = utils.XXH3FromBytes(contentBytes)

					if localHash != existingMeta.ContentHash {
						action = batchpool.ActionUpdate // Update if hash differs
						log.Printf("Detected UPDATE for %s (Local Hash: %s, Dify Hash: %s)", localRelPath, localHash, existingMeta.ContentHash)
					} else {
						// Hashes match, no update needed based on content
						// log.Printf("No change detected for %s", localRelPath)
						// Mark as complete in batch pool immediately if no action needed?
						// Or let the main loop handle only create/update/delete?
						// For simplicity, let's only queue C/U/D jobs.
						// We still need to mark progress for unchanged files.
						batchPool.MarkTaskComplete(key) // Mark unchanged file as 'complete' for progress
						totalFilesProcessed++
						continue // Skip job creation
					}
				}

				// Create and dispatch job if Create (0) or Update (1)
				if action == batchpool.ActionCreate || action == batchpool.ActionUpdate {
					job := Job{
						Type:       batchpool.Page, // Treat files as Pages
						RepoKey:    key,
						FilePath:   localRelPath,
						RepoPath:   repoLocalPath, // Pass absolute path to repo root
						Client:     difyClient,
						GitClient:  gitClient, // Pass Git client if needed by helpers (currently not)
						FileHash:   localHash, // Pass calculated hash (might be empty if create)
						DocumentID: "",        // Will be empty for create, set for update below
						Op: batchpool.Operation{
							Action:           action, // Use ActionType directly
							Type:             batchpool.Page,
							LastModifiedDate: localOp.LastModifiedDate, // From gitClient.ProcessRepository
							// DifyID and DatasetID will be set in document.go after API call
						},
					}
					if action == batchpool.ActionUpdate {
						job.DocumentID = existingMeta.DifyDocumentID // Set Dify ID for updates
					}

					// Send job to worker channel
					select {
					case jobChannels.Jobs <- job:
						totalJobsCreated++
						actionName := "UNKNOWN"
						switch action {
						case batchpool.ActionCreate:
							actionName = "CREATE"
						case batchpool.ActionUpdate:
							actionName = "UPDATE"
						}
						log.Printf("Dispatched %s job for %s", actionName, localRelPath)
					default:
						log.Printf("Warning: Job channel full. Blocking or dropping job for %s", localRelPath)
						// Handle channel full scenario if necessary (e.g., wait or log/drop)
						jobChannels.Jobs <- job // Retry sending (will block if full)
						totalJobsCreated++
					}
				}
				totalFilesProcessed++
			} // End loop through local files

			// 2. Iterate through Dify metadata: Check for Deletes
			// Remove unused docID variable from the loop declaration
			for _, meta := range difyMetadataMap {
				// Check if the file corresponding to this metadata still exists locally
				if _, exists := processedLocalFiles[meta.OriginalPath]; !exists {
					// File exists in Dify metadata but not in local processed list -> Delete
					log.Printf("Detected DELETE for %s (Dify ID: %s)", meta.OriginalPath, meta.DifyDocumentID)
					job := Job{
						Type:       batchpool.Page,
						RepoKey:    key,
						FilePath:   meta.OriginalPath, // Use path from metadata
						RepoPath:   repoLocalPath,
						Client:     difyClient,
						GitClient:  gitClient,
						DocumentID: meta.DifyDocumentID, // Dify ID is needed for deletion
						Op: batchpool.Operation{
							Action: batchpool.ActionDelete, // Delete
							Type:   batchpool.Page,
							DifyID: meta.DifyDocumentID, // Set Dify ID here for the operation log
							// LastModifiedDate not relevant for delete
						},
					}
					// Send delete job
					select {
					case jobChannels.Jobs <- job:
						totalJobsCreated++
						log.Printf("Dispatched DELETE job for %s", meta.OriginalPath)
					default:
						log.Printf("Warning: Job channel full. Blocking or dropping DELETE job for %s", meta.OriginalPath)
						jobChannels.Jobs <- job // Retry sending
						totalJobsCreated++
					}
					// Also mark as complete in batch pool? Deletes don't go through monitoring.
					// Let's assume SetTotal was based on local files, so deletes don't affect progress count.
				}
			} // End loop through Dify metadata

			log.Printf("Finished processing repository %s. Local Files Processed: %d, Jobs Created: %d", key, totalFilesProcessed, totalJobsCreated)

		} // End loop through repos in workspace
	} // End loop through workspaces
}
