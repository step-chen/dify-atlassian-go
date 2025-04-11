package utils

import (
	"fmt"
	"io"
	"os"

	"github.com/zeebo/xxh3"
)

// CalculateXXH3 computes the XXH3 hash of a file's content.
func CalculateXXH3(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	hasher := xxh3.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("failed to hash file %s: %w", filePath, err)
	}

	hashValue := hasher.Sum64()
	return fmt.Sprintf("%x", hashValue), nil // Return hash as hex string
}

// CalculateXXH3FromBytes computes the XXH3 hash of a byte slice.
func CalculateXXH3FromBytes(content []byte) string {
	hashValue := xxh3.Hash(content)
	return fmt.Sprintf("%x", hashValue) // Return hash as hex string
}
