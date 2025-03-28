package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	failedTypes   = make(map[string]bool) // Track failed media types
	failedTypesMu sync.Mutex              // Mutex for thread-safe access
)

// WriteFailedTypesLog writes failed media types to log
func WriteFailedTypesLog() {
	failedTypesMu.Lock()
	defer failedTypesMu.Unlock()

	if len(failedTypes) > 0 {
		log.Println("Failed to process these media types:")
		for mediaType := range failedTypes {
			log.Println("-", mediaType)
		}
	}
}

func generateTempFileName(fileName string) string {
	base := filepath.Base(fileName)
	ext := filepath.Ext(base)
	prefix := strings.TrimSuffix(base, ext)
	suffix := strings.TrimPrefix(ext, ".")

	return prefix + "-*." + suffix
}

func ConvertWithPandoc(inputPath string) (string, error) {
	outputExt := ".md"
	outputPath := inputPath[:len(inputPath)-len(filepath.Ext(inputPath))] + outputExt

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "pandoc", "-t", "markdown", inputPath, "-o", outputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return outputPath, fmt.Errorf("pandoc conversion timed out after 30 seconds")
		}
		return outputPath, fmt.Errorf("pandoc conversion failed: %w, output: %s", err, string(output))
	}

	return outputPath, nil
}

func ConvertWithMarkitdown(inputPath string) (string, error) {
	outputExt := ".md"
	outputPath := inputPath[:len(inputPath)-len(filepath.Ext(inputPath))] + outputExt

	// Open input file
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return "", fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return "", fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Prepare docker command
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", MarkitdownImage)
	cmd.Stdin = inputFile
	cmd.Stdout = outputFile

	// Capture stderr for error reporting
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// Run command
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("markitdown docker conversion failed: %w, stderr: %s", err, stderr.String())
	}

	return outputPath, nil
}

// PrepareAttachmentFile downloads a file from the given URL and saves it to a temporary file
// Returns the path to the saved file or an error if the operation fails
// ChangeFileExtension changes the extension of a file path to the specified new extension
// Returns the new path or an error if the input path is invalid or the new extension is empty
func ChangeFileExtension(filePath, newExt string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("file path cannot be empty")
	}
	if newExt == "" {
		return "", fmt.Errorf("new extension cannot be empty")
	}

	// Ensure new extension starts with a dot
	if !strings.HasPrefix(newExt, ".") {
		newExt = "." + newExt
	}

	// Remove existing extension and add new one
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	newPath := filePath[:len(filePath)-len(ext)] + newExt

	return newPath, nil
}

func PrepareAttachmentFile(url, apiKey, fileName string, mediaType string, allowedTypes map[string]bool) (showPath string, tmpPath string, err error) {
	// Create the HTTP request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set authorization header
	req.Header.Set("Authorization", "Bearer "+apiKey)

	// Make the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Save the file
	file, err := os.CreateTemp("", generateTempFileName(fileName))
	if err != nil {
		return "", "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return "", "", fmt.Errorf("failed to save file content: %w", err)
	}

	// Check if file type is allowed
	if allowedTypes[mediaType] {
		// Don't remove the file as it's in allowedTypes and should be preserved
		return fileName, file.Name(), nil
	}

	showPath, err = ChangeFileExtension(fileName, ".md")
	if err != nil {
		return "", "", fmt.Errorf("failed to change file extension: %w", err)
	}

	// Declare variables outside if block
	var convertedPath string
	var markitdownErr error

	// Try Markitdown first if image is available
	if dockerUtils.markitdownImage {
		convertedPath, markitdownErr = ConvertWithMarkitdown(file.Name())
		if markitdownErr == nil {
			os.Remove(file.Name()) // Remove original file after successful conversion
			return showPath, convertedPath, nil
		}
	} else {
		markitdownErr = fmt.Errorf("markitdown docker image not available")
	}

	// Fallback to Pandoc if image is available
	var pandocErr error
	if dockerUtils.pandocImage {
		convertedPath, pandocErr = ConvertWithPandoc(file.Name())
		if pandocErr == nil {
			os.Remove(file.Name()) // Remove original file after successful conversion
			return showPath, convertedPath, nil
		}
	} else {
		pandocErr = fmt.Errorf("pandoc docker image not available")
	}

	// Cleanup and return error
	os.Remove(file.Name()) // Remove file when both conversions fail
	failedTypesMu.Lock()
	failedTypes[mediaType] = true
	failedTypesMu.Unlock()

	return "", "", fmt.Errorf("all conversion attempts failed. Markitdown: %w, Pandoc: %w", markitdownErr, pandocErr)
}
