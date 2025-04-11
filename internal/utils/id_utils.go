package utils

import (
	"crypto/sha256"
	"encoding/hex"
)

// GenerateDocID creates a unique and consistent document identifier based on the file path.
// It uses SHA-256 hashing to ensure uniqueness and handle potential path complexities.
func GenerateDocID(filePath string) string {
	hasher := sha256.New()
	// Write the file path string to the hasher
	hasher.Write([]byte(filePath))
	// Get the SHA-256 hash sum
	hashBytes := hasher.Sum(nil)
	// Encode the hash bytes to a hexadecimal string
	return hex.EncodeToString(hashBytes)
}
