package git

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

// Config holds the application configuration.
type Config struct {
	config.Config `yaml:",inline"` // Embed common settings from parent config
	Git           Git              `yaml:"git"`
}

// WorkspaceConfig holds configuration for a specific workspace
type WorkspaceConfig struct {
	Repositories []string `yaml:"repositories"`
	ExcludePaths []string `yaml:"excludePaths"`
}

// GitConfig holds Git specific configuration.
type Git struct {
	BaseURL        string                     `yaml:"base_url"`
	Username       string                     `yaml:"username"`
	APIKey         string                     `yaml:"api_key"`
	FileExtensions []string                   `yaml:"file_extensions"`
	ExcludePaths   []string                   `yaml:"exclude_paths"`
	Workspaces     map[string]WorkspaceConfig `yaml:"workspace"`
	TargetDir      string                     `yaml:"target_dir"`
}

// LoadConfig reads the configuration file from the given path, substitutes environment variables,
// and unmarshals it into a Config struct.
func LoadConfig(path string) (*Config, error) {
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	// Substitute environment variables (e.g., ${VAR_NAME})
	//expandedYaml := os.ExpandEnv(string(yamlFile))

	var cfg Config
	//err = yaml.Unmarshal([]byte(expandedYaml), &cfg)
	err = yaml.Unmarshal([]byte(yamlFile), &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %w", path, err)
	}

	// Basic validation
	if cfg.Dify.BaseURL == "" {
		return nil, fmt.Errorf("dify.base_url is required")
	}
	if cfg.Dify.APIKey == "" {
		return nil, fmt.Errorf("dify.api_key is required")
	}
	if cfg.Git.BaseURL == "" {
		return nil, fmt.Errorf("git.base_url is required")
	}
	if cfg.Git.Username == "" {
		return nil, fmt.Errorf("git.username is required")
	}
	if cfg.Git.APIKey == "" {
		return nil, fmt.Errorf("git.api_key is required")
	}
	if len(cfg.Git.FileExtensions) == 0 {
		return nil, fmt.Errorf("git.file_extensions must contain at least one extension")
	}
	if len(cfg.Git.Workspaces) == 0 {
		return nil, fmt.Errorf("at least one workspace must be configured")
	}

	// Normalize file extensions (lowercase, ensure leading dot)
	normalizedExtensions := make([]string, len(cfg.Git.FileExtensions))
	for i, ext := range cfg.Git.FileExtensions {
		ext = strings.ToLower(strings.TrimSpace(ext))
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		normalizedExtensions[i] = ext
	}
	cfg.Git.FileExtensions = normalizedExtensions

	// Set default concurrency if not specified
	if cfg.Concurrency.Workers <= 0 {
		cfg.Concurrency.Workers = 5
	}
	if cfg.Concurrency.BatchPoolSize <= 0 {
		cfg.Concurrency.BatchPoolSize = 10
	}
	if cfg.Concurrency.QueueSize <= 0 {
		cfg.Concurrency.QueueSize = 4
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
