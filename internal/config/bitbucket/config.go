package bitbucket_cfg

import (
	"fmt"
	"os"

	"github.com/step-chen/dify-atlassian-go/internal/config" // Import the parent config package
	"gopkg.in/yaml.v3"
)

// RepoInfo contains repository configuration details
type RepoInfo struct {
	Project    string   `yaml:"project"`    // Project identifier
	ReposSlug  string   `yaml:"repos_slug"` // Repository slug
	Brantch    string   `yaml:"brantch"`    // Branch name (preserve YAML typo)
	Directory  string   `yaml:"directory"`  // Directory path in repository
	Filter     []string `yaml:"filter"`     // File filter patterns
	ParserType string   `yaml:"parser_type"`
	EOF        string   `yaml:"segment_eof"`
}

// Config contains settings for Bitbucket processing
type Config struct {
	config.Config `yaml:",inline"` // Embed common settings from parent config
	Bitbucket     Bitbucket        `yaml:"bitbucket"` // Bitbucket specific settings
}

// Bitbucket contains Bitbucket API integration settings
type Bitbucket struct {
	BaseURL string              `yaml:"base_url"` // Base URL for Bitbucket API
	APIKey  string              `yaml:"api_key"`  // API key for Bitbucket authentication
	Keys    map[string]RepoInfo `yaml:"keys"`     // Repository configuration by key
}

// GetDifyConfig returns the Dify configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetDifyConfig() config.DifyCfg {
	return c.Dify
}

// GetConcurrencyConfig returns the Concurrency configuration part, satisfying the DifyClientConfigProvider interface.
func (c *Config) GetConcurrencyConfig() config.ConcCfg {
	return c.Concurrency
}

// LoadConfig reads, validates and decrypts configuration for Bitbucket processing
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

	// Decrypt Bitbucket API Key using the Decrypt function from parent config package
	if cfg.Bitbucket.APIKey != "" {
		decryptedKey, err := config.Decrypt(cfg.Bitbucket.APIKey)
		if err != nil {
			// Handle decryption failure
			return nil, fmt.Errorf("failed to decrypt Bitbucket API key from '%s': %v", path, err)
		}
		cfg.Bitbucket.APIKey = decryptedKey
	} else {
		return nil, fmt.Errorf("Bitbucket API key is missing in the configuration file '%s'", path)
	}

	// Decrypt Dify API Key (if present and needs decryption here - depends on workflow)
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

	// Validate essential Bitbucket settings
	if cfg.Bitbucket.BaseURL == "" {
		return nil, fmt.Errorf("Bitbucket base_url is missing in '%s'", path)
	}
	if len(cfg.Bitbucket.Keys) == 0 {
		return nil, fmt.Errorf("no Bitbucket keys configured in '%s'", path)
	}

	// Validate Dify dataset mappings
	if len(cfg.Dify.Datasets) == 0 {
		return nil, fmt.Errorf("no Dify dataset mappings configured in '%s'", path)
	}
	for key := range cfg.Bitbucket.Keys {
		if _, exists := cfg.Dify.Datasets[key]; !exists {
			return nil, fmt.Errorf("no dataset_id configured for project '%s' in '%s'", key, path)
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
