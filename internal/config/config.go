package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// LogConfig defines logging configuration
type LogConfig struct {
	Level  string `yaml:"level"`  // Log level (debug, info, warn, error)
	Format string `yaml:"format"` // Log format (text, json)
}

type Config struct {
	Concurrency      ConcCfg                           `yaml:"concurrency"`       // Concurrency settings
	Dify             DifyCfg                           `yaml:"dify"`              // Dify API configuration
	AllowedTypes     map[string]utils.ConversionMethod `yaml:"allowed_types"`     // Allowed media types and their conversion methods
	UnsupportedTypes map[string]bool                   `yaml:"unsupported_types"` // Unsupported media types
	Log              LogConfig                         `yaml:"log"`               // Logging configuration
	AI               AIConfig                          `yaml:"ai"`
}

type AIConfig struct {
	URL       string `yaml:"url"`
	APIKey    string `yaml:"api_key"`
	ModelName string `yaml:"model_name"`
	Prompt    string `yaml:"prompt"`
}

// GetDifyConfig returns the Dify configuration part.
func (c *Config) GetDifyConfig() DifyCfg {
	return c.Dify
}

// GetConcurrencyConfig returns the Concurrency configuration part.
func (c *Config) GetConcurrencyConfig() ConcCfg {
	return c.Concurrency
}

// DifyClientConfigProvider defines an interface for providing necessary config to the Dify client.
type DifyCfgProvider interface {
	GetDifyConfig() DifyCfg
	GetConcurrencyConfig() ConcCfg
}

// ProcessRule configures document processing behavior
// Mode: Processing mode (automatic/custom)
// Rules: Custom processing rules
type ProcessRule struct {
	Mode  string `yaml:"mode" json:"mode"`   // Cleaning, segmentation mode (e.g. "automatic", "custom")
	Rules Rules  `yaml:"rules" json:"rules"` // Custom rules (in automatic mode, this field is empty)
}

// Rules contains document processing rules
// PreProcessingRules: List of preprocessing steps
// Segmentation: Main segmentation rules
// ParentMode: Parent document handling mode
// SubchunkSegmentation: Subchunk segmentation rules
type Rules struct {
	PreProcessingRules   []PreprocessingRules     `yaml:"pre_processing_rules" json:"pre_processing_rules"` // List of preprocessing rules
	Segmentation         SegmentationRule         `yaml:"segmentation" json:"segmentation"`                 // Segmentation rules
	ParentMode           string                   `yaml:"parent_mode" json:"parent_mode"`                   // e.g. "full-doc", "paragraph"
	SubchunkSegmentation SubchunkSegmentationRule `yaml:"subchunk_segmentation" json:"subchunk_segmentation"`
}

// PreprocessingRules configures individual preprocessing steps
// ID: Rule identifier
// Enabled: Whether the rule is active
type PreprocessingRules struct {
	ID      string `yaml:"id" json:"id"`
	Enabled bool   `yaml:"enabled" json:"enabled"`
}

// SegmentationRule configures document segmentation
// Separator: Text separator for segmentation
// MaxTokens: Maximum token count per segment
type SegmentationRule struct {
	Separator string `yaml:"separator" json:"separator"`   // Custom segment identifier
	MaxTokens int    `yaml:"max_tokens" json:"max_tokens"` // Maximum length (token)
}

// SubchunkSegmentationRule configures subchunk segmentation
// Separator: Text separator for subchunks
// MaxTokens: Maximum token count per subchunk
// ChunkOverlap: Token overlap between adjacent subchunks
type SubchunkSegmentationRule struct {
	Separator    string `yaml:"separator" json:"separator"`         // Segmentation identifier
	MaxTokens    int    `yaml:"max_tokens" json:"max_tokens"`       // Maximum length (tokens)
	ChunkOverlap int    `yaml:"chunk_overlap" json:"chunk_overlap"` // Overlap between adjacent chunks
}

// ConcCfg manages parallel processing settings
// Enabled: Whether concurrency is active
// Workers: Number of concurrent workers
// QueueSize: Size of processing queue
// BatchPoolSize: Maximum concurrent batches
// IndexingTimeout: Document indexing timeout (minutes)
// MaxRetries: Maximum retry attempts for failed documents
type ConcCfg struct {
	Enabled              bool `yaml:"enabled"`                // Whether concurrency is enabled
	Workers              int  `yaml:"workers"`                // Number of concurrent workers
	QueueSize            int  `yaml:"queue_size"`             // Size of the processing queue
	BatchPoolSize        int  `yaml:"batch_pool_size"`        // Maximum number of batches in the global pool
	IndexingTimeout      int  `yaml:"indexing_timeout"`       // Timeout for document indexing (in minutes)
	MaxRetries           int  `yaml:"max_retries"`            // Maximum number of retries for timeout documents
	DeleteTimeoutContent bool `yaml:"delete_timeout_content"` // Whether to delete timeout content
}

// DifyCfg contains Dify API integration settings
// BaseURL: Dify API endpoint
// APIKey: Authentication key
// Datasets: Space to dataset mappings
// RagSetting: Retrieval-Augmented Generation configuration
// KeywordWorkerQueueSize: Queue size for the keyword update worker.
type DifyCfg struct {
	BaseURL                string            `yaml:"base_url"`                  // Base URL for Dify API
	APIKey                 string            `yaml:"api_key"`                   // API key for authentication
	Datasets               map[string]string `yaml:"datasets"`                  // Mapping of space keys to dataset IDs
	RagSetting             RagSetting        `yaml:"rag_setting"`               // RAG settings
	KeywordWorkerQueueSize int               `yaml:"keyword_worker_queue_size"` // Queue size for the keyword update worker
}

// RagSetting configures Retrieval-Augmented Generation
// IndexingTechnique: Document indexing method
// DocForm: Document format
// ProcessRule: Document processing rules
type RagSetting struct {
	IndexingTechnique string       `yaml:"indexing_technique"` // Document indexing technique
	DocForm           string       `yaml:"doc_form"`           // Document form
	ProcessRule       *ProcessRule `yaml:"process_rule"`       // Document processing rules
}

// WARNING: This encryption key should be securely stored and rotated periodically
// Consider using a key management system for production environments
var encryptionKey = []byte("32-byte-long-encryption-key-here")

// Encrypt securely encrypts text using AES-256
// plaintext: Text to encrypt
// Returns base64 encoded ciphertext or error
// Parameters:
//   - plaintext: The text to encrypt
//
// Returns:
//   - string: Base64 encoded encrypted text
//   - error: Any error that occurred during encryption
func Encrypt(plaintext string) (string, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Never use more than 2^32 random nonces with a given key because of the risk of a repeat.
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Seal will encrypt the plaintext and append the authentication tag.
	// The nonce is prepended to the ciphertext for use during decryption.
	ciphertextBytes := gcm.Seal(nil, nonce, []byte(plaintext), nil)

	// Prepend nonce to the ciphertext
	nonceAndCiphertext := append(nonce, ciphertextBytes...)

	return base64.URLEncoding.EncodeToString(nonceAndCiphertext), nil
}

// Decrypt securely decrypts AES-256 encrypted text
// It expects the input ciphertext to be base64 encoded, with the nonce prepended to the actual ciphertext.
// ciphertext: Base64 encoded ciphertext
// Returns decrypted plaintext or error
// Parameters:
//   - ciphertext: Base64 encoded encrypted text
//
// Returns:
//   - string: Decrypted plaintext
//   - error: Any error that occurred during decryption
func Decrypt(ciphertext string) (string, error) {
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceAndCiphertextBytes, err := base64.URLEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(nonceAndCiphertextBytes) < nonceSize {
		return "", errors.New("ciphertext too short to contain nonce")
	}

	nonce := nonceAndCiphertextBytes[:nonceSize]
	actualCiphertext := nonceAndCiphertextBytes[nonceSize:]

	plaintextBytes, err := gcm.Open(nil, nonce, actualCiphertext, nil)
	if err != nil {
		// This error can occur if the ciphertext was tampered with (authentication failed)
		return "", fmt.Errorf("failed to decrypt or authenticate: %w", err)
	}

	return string(plaintextBytes), nil
}
