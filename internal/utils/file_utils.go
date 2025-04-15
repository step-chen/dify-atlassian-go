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
	"unicode"

	"github.com/cespare/xxhash/v2"
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

func FormatContent(s string) string {
	s = TrimString(s)
	s = strings.TrimLeft(s, ".")
	// Trim leading and trailing whitespace and non-printable characters
	return TrimString(s)
}

func FormatTitle(s string) string {
	s = TrimString(s)
	s = strings.TrimLeft(s, ".")
	// Replace underscores with spaces
	s = ReplaceUnderscoresWithSpaces(s)
	// Trim leading and trailing whitespace and non-printable characters
	return TrimString(s)
}

// EnsureTitleInMarkdown ensures the markdown content starts with the title
// If the content doesn't start with the title, adds it in markdown format
func EnsureTitleInContent(content, title, prefix, suffix string) string {
	s := FormatTitle(title)
	s = prefix + s + suffix
	if !strings.HasPrefix(content, title) {
		return s + content
	}
	return content
}

// TrimString removes leading and trailing meaningless characters from the input string
// including whitespace, control characters, and other non-printable characters
func TrimString(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		// Remove spaces, control characters, and other non-printable characters
		return unicode.IsSpace(r) || !unicode.IsGraphic(r)
	})
}

// ReplaceUnderscoresWithSpaces replaces all underscores in the input string with spaces
func ReplaceUnderscoresWithSpaces(s string) string {
	return strings.ReplaceAll(s, "_", " ")
}

// RemoveFileExtension removes the extension from a filename if it exists.
func RemoveFileExtension(fileName string) string {
	ext := filepath.Ext(fileName)
	if ext != "" {
		return strings.TrimSuffix(fileName, ext)
	}
	return fileName
}

func generateTempFileName(fileName string) string {
	base := filepath.Base(fileName)
	ext := filepath.Ext(base)
	prefix := strings.TrimSuffix(base, ext)
	suffix := strings.TrimPrefix(ext, ".")

	return prefix + "-*." + suffix
}

// convertToMarkdown calls pandoc to convert the specified file to Markdown text
// inputFilePath: path to the file to be converted
// Returns the converted Markdown string and a potential error
func convert2MarkdownByPandoc(inputFilePath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "pandoc", inputFilePath, "-t", "markdown")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("pandoc conversion timed out")
		}
		return "", fmt.Errorf("pandoc execution failed: %w\nstderr: %s", err, stderr.String())
	}

	return FormatContent(stdout.String()), nil
}

func convert2MarkdownByMarkitdown(inputPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	inputFile, err := os.Open(inputPath)
	if err != nil {
		return "", fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", MarkitdownImage)
	var stdout, stderr bytes.Buffer
	cmd.Stdin = inputFile
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("markitdown conversion timed out")
		}
		return "", fmt.Errorf("markitdown docker conversion failed: %w, stderr: %s", err, stderr.String())
	}

	return FormatContent(stdout.String()), nil
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

// ConvertHTMLToMarkdown converts an HTML string to Markdown using the markitdown Docker image.
func ConvertHTMLToMarkdown(htmlContent string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second) // 5-minute timeout
	defer cancel()

	// Prepare docker command
	// Assuming MarkitdownImage is defined elsewhere, e.g., as a constant or global variable like "kohirens/markitdown"
	// If MarkitdownImage is not defined, this will cause a compile error.
	// Replace "MarkitdownImage" with the actual image name if it's different or pass it as config.
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", MarkitdownImage, "-m", "text/html") // Example image name

	// Set stdin to the HTML content string
	cmd.Stdin = strings.NewReader(htmlContent)

	// Capture stdout and stderr
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run command
	err := cmd.Run()

	// Check for timeout first
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("markitdown docker conversion timed out after 300 seconds")
	}

	// Check for other errors
	if err != nil {
		return "", fmt.Errorf("markitdown docker conversion failed: %w, stderr: %s", err, stderr.String())
	}

	// Return the converted markdown content
	return FormatContent(stdout.String()), nil
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

// DownloadFileToTemp downloads a file from the given URL to a temporary location
// Returns the path to the temporary file or an error if the operation fails
func DownloadFileToTemp(url, apiKey, fileName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	var httpClient = &http.Client{}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	file, err := os.CreateTemp("", generateTempFileName(fileName))
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		os.Remove(file.Name()) // Clean up if copy fails
		return "", fmt.Errorf("failed to save file content: %w", err)
	}

	return file.Name(), nil
}

// PrepareAttachmentMarkdown downloads a file and converts it to Markdown text
// Returns the Markdown content or an error if the operation fails
func PrepareAttachmentMarkdown(url, apiKey, fileName, mediaType string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Download file to temp location
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	tmpPath, err := DownloadFileToTemp(url, apiKey, fileName)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer func() {
		if err := os.Remove(tmpPath); err != nil {
			log.Printf("warning: failed to remove temp file %s: %v", tmpPath, err)
		}
	}()

	var markdown string
	var conversionErr error

	// Try Markitdown first if image is available
	if dockerUtils.markitdownImage {
		markdown, conversionErr = convert2MarkdownByMarkitdown(tmpPath)
		if conversionErr == nil {
			return markdown, nil
		}
		log.Printf("markitdown conversion failed: %v", conversionErr)
	}

	// Fallback to Pandoc if image is available
	if dockerUtils.pandocImage {
		markdown, conversionErr = convert2MarkdownByPandoc(tmpPath)
		if conversionErr == nil {
			return markdown, nil
		}
		log.Printf("pandoc conversion failed: %v", conversionErr)
	}

	// If all conversions failed, track the media type and return error
	failedTypesMu.Lock()
	failedTypes[mediaType] = true
	failedTypesMu.Unlock()

	return "", fmt.Errorf("failed to convert file to Markdown: %w", conversionErr)
}

// XXH3Hash generates XXH3 hash for the input text
func XXH3Hash(text string) uint64 {
	return xxhash.Sum64String(text)
}
