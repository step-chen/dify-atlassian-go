package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	CFG "github.com/step-chen/dify-atlassian-go/internal/config/directory"
	"github.com/step-chen/dify-atlassian-go/internal/dify"
	AI "github.com/step-chen/dify-atlassian-go/internal/process-ai"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// processDirectory processes all files in a given directory
func processDirectory(cfgDir CFG.DirectoryPath, client *dify.Client, jobChan *JobChannels) error {
	// Get list of files
	files, err := getDirectoryFiles(cfgDir)
	if err != nil {
		return fmt.Errorf("error getting files for directory %s: %w", cfgDir.SourcePath, err)
	}

	// Initialize operations based on existing mappings
	if err := initOperations(client, files); err != nil {
		return fmt.Errorf("error initializing operations for directory %s: %w", cfgDir.SourcePath, err)
	}

	defer func() {
		log.Printf("=========================================================")
		log.Printf("All operations for directory %s have been processed.", cfgDir.SourcePath)
		log.Printf("=========================================================")
	}()

	// Ensure we close the channel when done
	defer close(jobChan.Jobs)

	batchPool.SetTotal(cfgDir.Name, len(files))

	// Preprocess all files first
	jobs := make([]*Job, 0, len(files))
	totalFiles := len(files)
	processed := 0
	errorCount := 0
	for filePath, operation := range files {
		job, err := preprocessingOperation(cfgDir, filePath, operation, client, jobChan)
		if err != nil {
			log.Printf("error preprocessing directory %s file %s: %v", cfgDir.SourcePath, filePath, err)
			errorCount++
		} else {
			if job != nil {
				jobs = append(jobs, job)
			}
			processed++
		}
		if processed%10 == 0 || processed == totalFiles {
			log.Printf("processed %d/%d files (%d errors) in directory %s",
				processed, totalFiles, errorCount, cfgDir.SourcePath)
		}
	}

	// Process all jobs after preprocessing
	for _, job := range jobs {
		if err := processOperation(job, jobChan); err != nil {
			log.Printf("error processing directory %s file %s: %v", cfgDir.SourcePath, job.RelativePath, err)
		}
	}

	return nil
}

// getDirectoryFiles retrieves all files in a directory that match allowed types
func getDirectoryFiles(cfgDir CFG.DirectoryPath) (map[string]batchpool.Operation, error) {
	files := make(map[string]batchpool.Operation)

	err := filepath.Walk(cfgDir.SourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Apply file filters if configured
		if len(cfgDir.Filter) > 0 {
			matched := false
			for _, pattern := range cfgDir.Filter {
				if matched, _ = filepath.Match(pattern, filepath.Base(path)); matched {
					break
				}
			}
			if !matched {
				return nil // Skip files that don't match any filter pattern
			}
		}

		// Check file type using MIMEType
		mimeType := utils.GetMIMEType(path)
		if cfg.AllowedTypes == nil {
			return nil
		}
		if _, exists := cfg.AllowedTypes[mimeType]; !exists {
			return nil // Skip unsupported types
		}
		if cfg.UnsupportedTypes != nil && cfg.UnsupportedTypes[mimeType] {
			return nil // Skip explicitly unsupported types
		}

		// Get file modification time
		modTime := info.ModTime().Format(time.RFC3339)

		fp := utils.RemoveRootDir(cfgDir.SourcePath, path)
		files[fp] = batchpool.Operation{
			Type:             batchpool.LocalFile,
			Action:           batchpool.ActionCreate, // Create by default
			LastModifiedDate: modTime,
			MediaType:        mimeType,
		}

		return nil
	})

	return files, err
}

// initOperations initializes file operations based on existing Dify mappings
func initOperations(client *dify.Client, files map[string]batchpool.Operation) error {
	// Fetch existing documents
	filePathToDifyRecord, err := client.FetchDocuments(0, 100)
	if err != nil {
		return fmt.Errorf("failed to list documents for %s: %v", client.BaseURL(), err)
	}

	// Iterate over existing records
	for filePath, record := range filePathToDifyRecord {
		if op, ok := files[filePath]; !ok {
			// Add delete operation for unmapped files
			files[filePath] = batchpool.Operation{
				Action: batchpool.ActionDelete,
				DifyID: record.DifyID,
			}
		} else {
			// Update existing operation
			op.DifyID = record.DifyID

			// Compare modification times
			equal := !utils.BeforeRFC3339Times(record.When, op.LastModifiedDate)

			// Determine action based on time comparison
			if !equal {
				op.Action = batchpool.ActionUpdate // Update if times differ
			} else {
				// Skip if no action needed
				delete(files, filePath)
				continue
			}

			files[filePath] = op
		}
	}

	return nil
}

// processOperation handles individual file operations
func preprocessingOperation(cfgDir CFG.DirectoryPath, relativePath string, operation batchpool.Operation, client *dify.Client, jobChan *JobChannels) (*Job, error) {
	if operation.Action == batchpool.ActionDelete {
		j := Job{
			Type:         operation.Type,
			DocumentID:   operation.DifyID,
			RootDir:      cfgDir.SourcePath,
			RelativePath: relativePath,
			Client:       client,
			Op:           operation,
			DirKey:       cfgDir.Name,
		}

		jobChan.Jobs <- j
		return nil, nil
	}

	// Read file content
	fp := filepath.Join(cfgDir.SourcePath, relativePath)

	// Read and parse HTML document
	fileContent, err := os.ReadFile(fp)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fp, err)
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(fileContent))
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML for file %s: %w", fp, err)
	}

	// Extract keywords from parsed document
	keywords := utils.ExtractKeywords(doc, cfgDir.Content.KeywordsBlocks)

	// Process HTML references
	htmlContent, err := utils.AppendHtmlRef(doc, cfgDir.ExcludedFilters, cfgDir.Content.ExcludedBlocks)
	if err != nil {
		return nil, fmt.Errorf("failed to process HTML references for file %s: %w", fp, err)
	}

	processedContent, err := AI.ProcessTextWithAIConfig(cfg.AI, htmlContent)
	if err != nil {
		return nil, fmt.Errorf("AI processing failed for file %s: %w", fp, err)
	}

	j := Job{
		Type:         operation.Type,
		Keywords:     keywords,
		DocumentID:   operation.DifyID,
		RootDir:      cfgDir.SourcePath,
		RelativePath: relativePath,
		Client:       client,
		Op:           operation,
		DirKey:       cfgDir.Name,
		// Content and PreprocessingFile are set below
	}

	// Write processed content to output directory if configured
	if cfgDir.OutputPath != "" {
		outputPath := filepath.Join(cfgDir.OutputPath, relativePath+".md")
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory for %s: %w", outputPath, err)
		}

		if err := os.WriteFile(outputPath, []byte(processedContent), 0644); err != nil {
			return nil, fmt.Errorf("failed to write markdown output file %s: %w", outputPath, err)
		}
		j.PreprocessingFile = outputPath
	} else {
		j.Content = processedContent // Store content directly in the job if no output path is specified
	}

	return &j, nil
}

// processOperation handles individual file operations
func processOperation(j *Job, jobChan *JobChannels) error {
	buf, err := os.ReadFile(j.PreprocessingFile)
	if err != nil {
		return err
	}
	j.Content = string(buf)

	jobChan.Jobs <- *j
	return nil
}

// createDocument creates a new document in Dify
func createDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Create new document
	docRequest := dify.CreateDocumentRequest{
		Name:              filepath.Base(j.RelativePath),
		Text:              string(j.Content),
		IndexingTechnique: cfg.Dify.RagSetting.IndexingTechnique,
		DocForm:           cfg.Dify.RagSetting.DocForm,
	}

	resp, err := j.Client.CreateDocumentByText(&docRequest)

	if err != nil {
		log.Printf("failed to create Dify document for directory %s file %s: %v", j.DirKey, j.RelativePath, err)
		return err
	}

	j.Op.DifyID = resp.Document.ID
	j.Op.StartAt = time.Now()
	j.Client.SetHashMapping(j.Client.GetHashByDifyIDFromRecord(resp.Document.ID), j.Op.DifyID)

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		IDToAdd:    j.RelativePath,
		URL:        j.RelativePath,
		SourceType: "directory",
		Type:       "file",
		When:       j.Op.LastModifiedDate,
		//When:       time.Now().Format(time.RFC3339),
		Xxh3: j.Client.GetHashByDifyIDFromRecord(resp.Document.ID),
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, "file", params); err != nil {
		// Pass file path during cleanup deletion attempt
		if errDel := j.Client.DeleteDocument(resp.Document.ID, "file", j.RelativePath); errDel != nil {
			log.Printf("failed to delete/update Dify document %s after metadata update failure: %v", resp.Document.ID, errDel)
		}
		return err
	}

	// Add document to batch pool for indexing tracking
	err = batchPool.Add(context.Background(), j.DirKey, j.RelativePath, docRequest.Name, resp.Batch, j.Op)
	if err != nil {
		// Log error if adding to the pool fails (e.g., pool shutdown)
		log.Printf("Error adding task to batch pool for directory %s file %s: %v", j.DirKey, j.RelativePath, err)
		// Consider how to handle this - should the document be deleted? For now, just log.
	}

	return nil // Return nil even if adding to pool failed, as document creation succeeded
}

// updateDocument updates an existing document in Dify
func updateDocument(j *Job) error {
	var resp *dify.CreateDocumentResponse

	// Update document
	updateRequest := dify.UpdateDocumentRequest{
		Name: filepath.Base(j.RelativePath),
		Text: string(j.Content),
	}

	resp, err := j.Client.UpdateDocumentByText(j.DocumentID, &updateRequest)

	if err != nil {
		log.Printf("failed to update Dify document for directory %s file %s: %v", j.DirKey, j.RelativePath, err)
		return err
	}

	j.Op.StartAt = time.Now()

	// Update document metadata using the new struct
	params := dify.DocumentMetadataRecord{
		URL:        j.RelativePath,
		SourceType: "directory",
		Type:       "file",
		When:       time.Now().Format(time.RFC3339),
		Xxh3:       j.Client.GetHashByDifyIDFromRecord(j.DocumentID),
	}
	if err := j.Client.UpdateDocumentMetadata(resp.Document.ID, "file", params); err != nil {
		return err
	}

	// Add document to batch pool for indexing tracking
	err = batchPool.Add(context.Background(), j.DirKey, j.RelativePath, updateRequest.Name, resp.Batch, j.Op)
	if err != nil {
		log.Printf("Error adding task to batch pool for directory %s file %s: %v", j.DirKey, j.RelativePath, err)
	}

	return nil // Return nil even if adding to pool failed, as document update succeeded
}

// deleteDocument deletes a document from Dify
func deleteDocument(j *Job) error {
	// Determine the file path based on the job type
	var relativePath string
	if j.Content != "" {
		relativePath = j.RelativePath
	} else {
		// Should not happen if job is constructed correctly
		log.Printf("Error: Could not determine file path for delete job with Dify ID %s", j.DocumentID)
		// Fallback: attempt deletion without specific file path? Or return error?
		// Let's return an error as the DeleteDocument logic now relies on it.
		return fmt.Errorf("could not determine file path for delete operation on Dify document %s", j.DocumentID)
	}

	// Delete document or update metadata
	err := j.Client.DeleteDocument(j.DocumentID, "file", relativePath)
	if err != nil {
		log.Printf("failed to delete/update Dify document %s (for file path %s): %v", j.DocumentID, relativePath, err)
		// Still return the error if deletion/update failed
		return err
	}

	// Log deletion success. ProgressString still works for getting current progress.
	log.Printf("%s Successfully deleted Dify document: %s", batchPool.ProgressString(j.DirKey), j.DocumentID)
	// Note: Since deletion doesn't involve batch monitoring, it completes immediately.
	// The BatchPool's total count for the directory (set via SetTotal) should account for this.
	// If SetTotal counts only items needing monitoring, deletions shouldn't affect its count.
	// If SetTotal counts *all* operations (create/update/delete), then the BatchPool's
	// completed count won't reach the total unless deletions are also marked complete somehow.
	// Let's assume SetTotal counts only monitorable tasks (create/update/upload).
	return nil
}
