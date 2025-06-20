package utils

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// CalculateXXH3FromBytes computes the XXH3 hash of a byte slice.
func XXH3FromBytes(content []byte) string {
	return fmt.Sprintf("%d", xxhash.Sum64(content))
}

// XXH3Hash generates XXH3 hash for the input text
func XXH3Hash(text string) string {
	return fmt.Sprintf("%d", xxhash.Sum64String(text))
}
