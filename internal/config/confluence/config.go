package confluence_cfg

import (
	"fmt"
	"os"

	"github.com/step-chen/dify-atlassian-go/internal/config" // Import the parent config package
	"gopkg.in/yaml.v2"
)

// Config contains settings specific to Confluence processing
type Config struct {
	BaseConfig         `yaml:",inline"`   // Embed common settings
	ConfluenceSettings ConfluenceSettings `yaml:"confluence"`        // Confluence specific settings
	AllowedTypes       map[string]bool    `yaml:"allowed_types"`     // Allowed media types for attachments (Moved here for context)
	UnsupportedTypes   map[string]bool    `yaml:"unsupported_types"` // Unsupported media types (Moved here for context)
}

// BaseConfig holds common configuration sections used by different commands
// It references types defined in the parent config package.
type BaseConfig struct {
	Concurrency config.ConcCfg `yaml:"concurrency"` // Concurrency settings from parent config
	Dify        config.DifyCfg `yaml:"dify"`        // Dify API configuration from parent config
}

// ConfluenceSettings contains Confluence API integration settings
// BaseURL: Confluence API endpoint
// APIKey: Authentication key
// SpaceKeys: List of spaces to process
type ConfluenceSettings struct {
	BaseURL   string   `yaml:"base_url"`   // Base URL for Confluence API
	APIKey    string   `yaml:"api_key"`    // API key for authentication (encrypted)
	SpaceKeys []string `yaml:"space_keys"` // List of space keys to process
	OnlyTitle bool     `yaml:"only_title"` // Whether to process only titles
}

// GetDifyConfig returns the Dify configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetDifyConfig() config.DifyCfg {
	return c.Dify
}

// GetConcurrencyConfig returns the Concurrency configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetConcurrencyConfig() config.ConcCfg {
	return c.Concurrency
}

// LoadConfig reads, validates and decrypts configuration for Confluence processing
// path: Path to YAML configuration file relative to the command's execution directory
// Returns parsed configuration or error
func LoadConfig(path string) (*Config, error) {
	configData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %v", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config from '%s': %v", path, err)
	}

	// Decrypt Confluence API Key using the Decrypt function from the parent config package
	if cfg.ConfluenceSettings.APIKey != "" {
		// Use config.Decrypt from the imported parent package
		decryptedKey, err := config.Decrypt(cfg.ConfluenceSettings.APIKey)
		if err != nil {
			// Handle decryption failure
			return nil, fmt.Errorf("failed to decrypt Confluence API key from '%s': %v", path, err)
		}
		cfg.ConfluenceSettings.APIKey = decryptedKey
	} else {
		return nil, fmt.Errorf("Confluence API key is missing in the configuration file '%s'", path)
	}

	// Decrypt Dify API Key (if present and needs decryption here - depends on workflow)
	// Assuming Dify key might also be encrypted in the same file
	if cfg.Dify.APIKey != "" {
		decryptedKey, err := config.Decrypt(cfg.Dify.APIKey)
		if err != nil {
			// Allow fallback to plain text for Dify key? Or fail? Let's fail for consistency.
			return nil, fmt.Errorf("failed to decrypt Dify API key from '%s': %v", path, err)
		}
		cfg.Dify.APIKey = decryptedKey
	} else {
		return nil, fmt.Errorf("Dify API key is missing in the configuration file '%s'", path)
	}

	// Apply default concurrency settings if needed
	if !cfg.Concurrency.Enabled {
		cfg.Concurrency.Workers = 1
		cfg.Concurrency.QueueSize = 1
	}
	if cfg.Concurrency.BatchPoolSize == 0 {
		cfg.Concurrency.BatchPoolSize = 10 // Default value
	}
	if cfg.Concurrency.IndexingTimeout == 0 {
		cfg.Concurrency.IndexingTimeout = 5 // Default timeout in minutes
	}
	if cfg.Concurrency.MaxRetries == 0 {
		cfg.Concurrency.MaxRetries = 2 // Default retries
	}

	// Validate essential Confluence settings
	if cfg.ConfluenceSettings.BaseURL == "" {
		return nil, fmt.Errorf("Confluence base_url is missing in '%s'", path)
	}
	if len(cfg.ConfluenceSettings.SpaceKeys) == 0 {
		return nil, fmt.Errorf("no Confluence space_keys configured in '%s'", path)
	}

	// Validate Dify dataset mappings
	if len(cfg.Dify.Datasets) == 0 {
		return nil, fmt.Errorf("no Dify dataset mappings configured in '%s'", path)
	}
	for _, spaceKey := range cfg.ConfluenceSettings.SpaceKeys {
		if _, exists := cfg.Dify.Datasets[spaceKey]; !exists {
			return nil, fmt.Errorf("no dataset_id configured for space key '%s' in '%s'", spaceKey, path)
		}
	}

	return &cfg, nil
}
