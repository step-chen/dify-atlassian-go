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
	Paths []DirectoryPath `yaml:"path"` // List of directory configurations
}

type DirectoryPath struct {
	Name            string     `yaml:"name"`             // Name identifier for the directory
	SourcePath      string     `yaml:"source_path"`      // Source directory path
	OutputPath      string     `yaml:"output_path"`      // Output directory path
	Filter          []string   `yaml:"filter"`           // File filters to include
	ExcludedFilters []string   `yaml:"excluded_filters"` // Filters to exclude
	Content         ContentCfg `yaml:"content"`          // Content block configuration
}

// ContentCfg defines supported and unsupported text blocks
type ContentCfg struct {
	KeywordsBlocks  []string `yaml:"keywords_blocks"`
	SupportedBlocks []string `yaml:"supported_blocks"` // Supported text blocks for processing
	ExcludedBlocks  []string `yaml:"excluded_blocks"`  // Unsupported text blocks that will be skipped
}

// LoadConfig reads, validates and decrypts configuration for directory processing
func LoadConfig(path string) (*Config, error) {
	configData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := decryptAPIKey(&cfg, path); err != nil {
		return nil, err
	}

	setConcurrencyDefaults(&cfg)
	setLogDefaults(&cfg)

	if err := validateDirectoryPaths(&cfg); err != nil {
		return nil, err
	}
	if err := validateDatasetMappings(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// decryptAPIKey handles decryption of the Dify API key
func decryptAPIKey(cfg *Config, path string) error {
	if cfg.Dify.APIKey == "" {
		return fmt.Errorf("Dify API key is missing in the config")
	}
	decryptedKey, err := config.Decrypt(cfg.Dify.APIKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt Dify API key: %w", err)
	}
	cfg.Dify.APIKey = decryptedKey
	return nil
}

// setConcurrencyDefaults ensures safe minimum values even when Enabled is true
func setConcurrencyDefaults(cfg *Config) {
	if !cfg.Concurrency.Enabled {
		cfg.Concurrency.Workers = 1
		cfg.Concurrency.QueueSize = 1
	} else {
		if cfg.Concurrency.Workers <= 0 {
			cfg.Concurrency.Workers = 1
		}
		if cfg.Concurrency.QueueSize <= 0 {
			cfg.Concurrency.QueueSize = 1
		}
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
}

// setLogDefaults applies default log level and format
func setLogDefaults(cfg *Config) {
	if cfg.Log.Level == "" {
		cfg.Log.Level = "info"
	}
	if cfg.Log.Format == "" {
		cfg.Log.Format = "text"
	}
}

// validateDirectoryPaths ensures required paths exist
func validateDirectoryPaths(cfg *Config) error {
	if len(cfg.Directory.Paths) == 0 {
		return fmt.Errorf("no directory paths configured")
	}
	return nil
}

// validateDatasetMappings checks that all paths have a matching dataset mapping
func validateDatasetMappings(cfg *Config) error {
	if len(cfg.Dify.Datasets) == 0 {
		return fmt.Errorf("no Dify dataset mappings configured")
	}
	for _, cfgPath := range cfg.Directory.Paths {
		if _, exists := cfg.Dify.Datasets[cfgPath.Name]; !exists {
			return fmt.Errorf("no dataset_id configured for directory '%s'", cfgPath.Name)
		}
	}
	return nil
}
