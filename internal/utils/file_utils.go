package utils

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

// GetMIMEType returns the MIME type based on file extension
func GetMIMEType(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return "application/octet-stream"
	}

	// Lookup MIME type by extension
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		return "application/octet-stream"
	}

	// Remove any charset parameters
	if i := strings.IndexByte(mimeType, ';'); i >= 0 {
		mimeType = mimeType[:i]
	}

	return mimeType
}

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

func formatContent(s string, separator string) string {
	s = trimString(s)
	s = strings.TrimLeft(s, ".")
	s = strings.TrimSuffix(s, "{}")

	if separator != "" {
		re := regexp.MustCompile(`(?m)^(##)\s+`)
		s = re.ReplaceAllString(s, separator+"\n## ")
		re = regexp.MustCompile(`(?m)^(#)\s+`)
		s = re.ReplaceAllString(s, separator+"\n# ")
	}

	// Trim leading and trailing whitespace and non-printable characters
	return trimString(s)
}

func RemoveRootDir(root, path string) string {
	dir := strings.TrimPrefix(path, root)
	dir = strings.TrimPrefix(dir, "\\")
	dir = strings.TrimPrefix(dir, "/")
	return dir
}

func RemoveFile(inputPath string) {
	fileInfo, err := os.Stat(inputPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("warning: failed to check file %s: %v", inputPath, err)
		}
		return
	}

	if fileInfo.IsDir() {
		log.Printf("warning: %s is a directory, not removing", inputPath)
		return
	}

	if err := os.Remove(inputPath); err != nil {
		log.Printf("warning: failed to remove temp file %s: %v", inputPath, err)
	}
}

func FormatTitle(s string) string {
	s = trimString(s)
	s = strings.TrimLeft(s, ".")
	// Replace underscores with spaces
	s = replaceUnderscoresWithSpaces(s)
	// Trim leading and trailing whitespace and non-printable characters
	return trimString(s)
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
func trimString(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		// Remove spaces, control characters, and other non-printable characters
		return unicode.IsSpace(r) || !unicode.IsGraphic(r)
	})
}

// ReplaceUnderscoresWithSpaces replaces all underscores in the input string with spaces
func replaceUnderscoresWithSpaces(s string) string {
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
