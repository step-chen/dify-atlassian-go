package localfolder_cfg

import (
	"fmt"
	"os"

	"github.com/step-chen/dify-atlassian-go/internal/config" // Import the parent config package
	"gopkg.in/yaml.v2"
)

// Config contains settings specific to local folder processing
type Config struct {
	BaseConfig  `yaml:",inline"`    // Embed common settings
	LocalFolder LocalFolderSettings `yaml:"local_folder"` // Local folder specific settings
	Folders     []FolderMapping     `yaml:"folders"`      // List of folders to process
}

// BaseConfig holds common configuration sections used by different commands
// It references types defined in the parent config package.
type BaseConfig struct {
	Concurrency config.ConcCfg `yaml:"concurrency"` // Concurrency settings from parent config
	Dify        config.DifyCfg `yaml:"dify"`        // Dify API configuration from parent config
}

// LocalFolderSettings defines parameters for handling local files
type LocalFolderSettings struct {
	SupportedExtensions []string `yaml:"supported_extensions"` // List of file extensions to process (e.g., [".md", ".txt"])
	MaxFileSizeMB       int64    `yaml:"max_file_size_mb"`     // Maximum file size in megabytes
}

// FolderMapping maps a local folder path to Dify dataset IDs
type FolderMapping struct {
	Path    string               `yaml:"path"`    // Path to the local folder
	Dataset config.DatasetConfig `yaml:"dataset"` // Corresponding Dify dataset IDs
}

// GetDifyConfig returns the Dify configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetDifyConfig() config.DifyCfg {
	return c.Dify
}

// GetConcurrencyConfig returns the Concurrency configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetConcurrencyConfig() config.ConcCfg {
	return c.Concurrency
}

// LoadConfig reads, validates and decrypts configuration for local folder processing
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

	// Decrypt Dify API Key using the Decrypt function from the parent config package
	if cfg.Dify.APIKey != "" {
		// Use config.Decrypt from the imported parent package
		decryptedKey, err := config.Decrypt(cfg.Dify.APIKey)
		if err != nil {
			// Handle decryption failure (e.g., log warning, use as plain text)
			fmt.Printf("Warning: Failed to decrypt Dify API key from '%s', attempting to use as plain text: %v\n", path, err)
			// Keep the original key if decryption fails
		} else {
			cfg.Dify.APIKey = decryptedKey
		}
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

	// Set default max file size if not specified
	if cfg.LocalFolder.MaxFileSizeMB == 0 {
		cfg.LocalFolder.MaxFileSizeMB = 10 // Default to 10MB
	}

	// Validate folder paths and dataset IDs
	if len(cfg.Folders) == 0 {
		return nil, fmt.Errorf("no folders configured for processing in '%s'", path)
	}
	for i, folder := range cfg.Folders {
		if folder.Path == "" {
			return nil, fmt.Errorf("folder path is missing for folder entry %d in '%s'", i, path)
		}
		if folder.Dataset.Content == "" {
			return nil, fmt.Errorf("dataset Content ID is missing for folder path '%s' in '%s'", folder.Path, path)
		}
		if folder.Dataset.Title == "" {
			// Optional? Depending on requirements. Let's make it required for now.
			return nil, fmt.Errorf("dataset Title ID is missing for folder path '%s' in '%s'", folder.Path, path)
		}
		// Basic check if path exists - more robust checks might be needed
		if _, err := os.Stat(folder.Path); os.IsNotExist(err) {
			// Log warning instead of failing? Depends on desired behavior.
			// For now, let's return an error.
			return nil, fmt.Errorf("configured folder path does not exist: '%s'", folder.Path)
		}
	}

	return &cfg, nil
}
