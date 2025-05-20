package directory_cfg

import (
	"fmt"
	"os"

	"github.com/step-chen/dify-atlassian-go/internal/config"
	"gopkg.in/yaml.v3"
)

// Config contains settings specific to directory processing
type Config struct {
	config.Config `yaml:",inline"` // Embed common settings from parent config
	Directory     Directory        `yaml:"directory"` // Directory specific settings
}

// Directory contains directory processing settings
type Directory struct {
	Path           map[string]string `yaml:"path"`            // Mapping of directory names to paths
	FileExtensions []string          `yaml:"file_extensions"` // Allowed file extensions
}

// LoadConfig reads, validates and decrypts configuration for directory processing
func LoadConfig(path string) (*Config, error) {
	configData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %v", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config from '%s': %v", path, err)
	}

	// Decrypt Dify API Key
	if cfg.Dify.APIKey != "" {
		decryptedKey, err := config.Decrypt(cfg.Dify.APIKey)
		if err != nil {
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
		cfg.Concurrency.BatchPoolSize = 10
	}
	if cfg.Concurrency.IndexingTimeout == 0 {
		cfg.Concurrency.IndexingTimeout = 5
	}
	if cfg.Concurrency.MaxRetries == 0 {
		cfg.Concurrency.MaxRetries = 2
	}

	// Validate directory paths
	if len(cfg.Directory.Path) == 0 {
		return nil, fmt.Errorf("no directory paths configured in '%s'", path)
	}

	// Validate Dify dataset mappings
	if len(cfg.Dify.Datasets) == 0 {
		return nil, fmt.Errorf("no Dify dataset mappings configured in '%s'", path)
	}
	for dirName := range cfg.Directory.Path {
		if _, exists := cfg.Dify.Datasets[dirName]; !exists {
			return nil, fmt.Errorf("no dataset_id configured for directory '%s' in '%s'", dirName, path)
		}
	}

	// Set default log level and format if not specified
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}
	if cfg.Log.Format == "" {
		cfg.Log.Format = "text"
	}

	return &cfg, nil
}
