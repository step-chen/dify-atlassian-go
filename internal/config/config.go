package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v2"
)

// Config represents the application's configuration structure
type Config struct {
	Confluence       ConfluenceConfig  `yaml:"confluence"`        // Confluence API configuration
	Concurrency      ConcurrencyConfig `yaml:"concurrency"`       // Concurrency settings
	Dify             DifyConfig        `yaml:"dify"`              // Dify API configuration
	AllowedTypes     map[string]bool   `yaml:"allowed_types"`     // Allowed media types for attachments
	UnsupportedTypes map[string]bool   `yaml:"unsupported_types"` // Unsupported media types
	ProcessRule      *ProcessRule      `yaml:"process_rule"`      // Document processing rules
}

// ProcessRule defines document processing rules configuration
type ProcessRule struct {
	Mode  string `yaml:"mode" json:"mode"`   // Cleaning, segmentation mode (e.g. "automatic", "custom")
	Rules Rules  `yaml:"rules" json:"rules"` // Custom rules (in automatic mode, this field is empty)
}

// Rules defines the rules structure for document processing
type Rules struct {
	PreProcessingRules   []PreprocessingRules     `yaml:"pre_processing_rules" json:"pre_processing_rules"` // List of preprocessing rules
	Segmentation         SegmentationRule         `yaml:"segmentation" json:"segmentation"`                 // Segmentation rules
	ParentMode           string                   `yaml:"parent_mode" json:"parent_mode"`                   // e.g. "full-doc", "paragraph"
	SubchunkSegmentation SubchunkSegmentationRule `yaml:"subchunk_segmentation" json:"subchunk_segmentation"`
}

// PreprocessingRule defines preprocessing rules
type PreprocessingRules struct {
	ID      string `yaml:"id" json:"id"`
	Enabled bool   `yaml:"enabled" json:"enabled"`
}

// SegmentationRule defines segmentation rules
type SegmentationRule struct {
	Separator string `yaml:"separator" json:"separator"`   // Custom segment identifier
	MaxTokens int    `yaml:"max_tokens" json:"max_tokens"` // Maximum length (token)
}

// SubchunkSegmentationRule defines subchunk segmentation rules
type SubchunkSegmentationRule struct {
	Separator    string `yaml:"separator" json:"separator"`         // Segmentation identifier
	MaxTokens    int    `yaml:"max_tokens" json:"max_tokens"`       // Maximum length (tokens)
	ChunkOverlap int    `yaml:"chunk_overlap" json:"chunk_overlap"` // Overlap between adjacent chunks
}

// ConcurrencyConfig defines concurrency settings
type ConcurrencyConfig struct {
	Enabled             bool `yaml:"enabled"`               // Whether concurrency is enabled
	Workers             int  `yaml:"workers"`               // Number of concurrent content workers
	QueueSize           int  `yaml:"queue_size"`            // Size of the content processing queue
	AttachmentWorkers   int  `yaml:"attachment_workers"`    // Number of concurrent attachment workers
	AttachmentQueueSize int  `yaml:"attachment_queue_size"` // Size of the attachment processing queue
	DeleteWorkers       int  `yaml:"delete_workers"`        // Number of concurrent delete workers
	DeleteQueueSize     int  `yaml:"delete_queue_size"`     // Size of the delete processing queue
	BatchPoolSize       int  `yaml:"batch_pool_size"`       // Maximum number of batches in the global pool
	IndexingTimeout     int  `yaml:"indexing_timeout"`      // Timeout for document indexing (in minutes)
	MaxRetries          int  `yaml:"max_retries"`           // Maximum number of retries for timeout documents
}

// DifyConfig defines Dify API configuration
type DifyConfig struct {
	BaseURL  string            `yaml:"base_url"` // Base URL for Dify API
	APIKey   string            `yaml:"api_key"`  // API key for authentication
	Datasets map[string]string `yaml:"datasets"` // Mapping of space keys to dataset IDs
}

// ConfluenceConfig defines Confluence API configuration
type ConfluenceConfig struct {
	BaseURL   string   `yaml:"base_url"`   // Base URL for Confluence API
	APIKey    string   `yaml:"api_key"`    // API key for authentication
	SpaceKeys []string `yaml:"space_keys"` // List of space keys to process
}

// WARNING: This encryption key should be securely stored and rotated periodically
var encryptionKey = []byte("32-byte-long-encryption-key-here")

// LoadConfig loads and validates the configuration from the specified path
// Parameters:
//   - path: Path to the configuration file
//
// Returns:
//   - *Config: Loaded configuration
//   - error: Any error that occurred during loading
//
// The function also decrypts the Confluence API key during loading
func LoadConfig(path string) (*Config, error) {
	configData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	// Decrypt API Key
	decryptedKey, err := Decrypt(cfg.Confluence.APIKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt API key: %v", err)
	}
	cfg.Confluence.APIKey = decryptedKey

	// If concurrency is not enabled, set single-thread mode
	if !cfg.Concurrency.Enabled {
		cfg.Concurrency.Workers = 1
		cfg.Concurrency.QueueSize = 1
		cfg.Concurrency.AttachmentWorkers = 1
		cfg.Concurrency.AttachmentQueueSize = 1
		cfg.Concurrency.DeleteWorkers = 1
		cfg.Concurrency.DeleteQueueSize = 1
	}

	// Set default batch pool size if not specified
	if cfg.Concurrency.BatchPoolSize == 0 {
		cfg.Concurrency.BatchPoolSize = 10
	}

	return &cfg, nil
}

// Encrypt encrypts the given plaintext using AES encryption
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

	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], []byte(plaintext))

	return base64.URLEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypts the given ciphertext using AES decryption
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

	decryptedData, err := base64.URLEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	if len(decryptedData) < aes.BlockSize {
		return "", errors.New("ciphertext too short")
	}

	iv := decryptedData[:aes.BlockSize]
	decryptedData = decryptedData[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(decryptedData, decryptedData)

	return string(decryptedData), nil
}
