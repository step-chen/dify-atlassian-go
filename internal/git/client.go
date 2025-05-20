package git

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/config/git"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

type Client struct {
	cfg *git.Config
}

func NewClient(config *git.Config) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	return &Client{
		cfg: config,
	}, nil
}

// CloneRepositories clones all configured repositories
func (c *Client) CloneRepositories() error {
	for workspace, config := range c.cfg.Git.Workspaces {
		workspaceDir := filepath.Join(c.cfg.Git.TargetDir, workspace)
		if err := os.MkdirAll(workspaceDir, 0755); err != nil {
			return fmt.Errorf("failed to create workspace directory: %w", err)
		}

		for _, repo := range config.Repositories {
			baseURL := c.cfg.Git.BaseURL
			// Remove any trailing slashes from base URL
			baseURL = strings.TrimRight(baseURL, "/")
			// Split base URL into protocol and host
			parts := strings.Split(baseURL, "://")
			protocol := parts[0]
			host := parts[1]
			// Construct repository URL with authentication
			repoURL := fmt.Sprintf("%s://%s:%s@%s/%s/%s",
				protocol, c.cfg.Git.Username, c.cfg.Git.APIKey, host, workspace, repo)
			repoPath := filepath.Join(workspaceDir, repo)
			if _, err := os.Stat(repoPath); err == nil {
				// Repository exists, pull latest changes
				if err := utils.GitPull(repoPath); err != nil {
					return fmt.Errorf("failed to pull repository %s: %w", repo, err)
				}
			} else if os.IsNotExist(err) {
				// Repository doesn't exist, clone it
				if err := utils.GitClone(repoURL, repoPath, "", ""); err != nil {
					return fmt.Errorf("failed to clone repository %s: %w", repo, err)
				}
			} else {
				// Other error
				return fmt.Errorf("failed to check repository directory %s: %w", repo, err)
			}
		}
	}
	return nil
}

// ProcessRepository processes a cloned repository
func (c *Client) ProcessRepository(workspace, repository string) (map[string]batchpool.Operation, error) {
	repoPath := filepath.Join(c.cfg.Git.TargetDir, workspace, repository)
	if !utils.IsGitRepo(repoPath) {
		return nil, fmt.Errorf("%s is not a git repository", repoPath)
	}

	contents := make(map[string]batchpool.Operation)
	err := filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip excluded paths
		for _, exclude := range c.cfg.Git.ExcludePaths {
			if strings.Contains(path, exclude) {
				return nil
			}
		}

		// Process files with allowed extensions
		if !info.IsDir() && c.isAllowedFile(path) {
			relPath, _ := filepath.Rel(repoPath, path)
			contents[relPath] = batchpool.Operation{
				Action:           0,
				LastModifiedDate: info.ModTime().Format(time.RFC3339),
				Type:             0,
			}
		}
		return nil
	})

	return contents, err
}

func (c *Client) isAllowedFile(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	for _, allowed := range c.cfg.Git.FileExtensions {
		if ext == allowed {
			return true
		}
	}
	return false
}

// GetFileContent retrieves the content of a file from the Git repository.
func (c *Client) GetFileContent(filePath string) (*Content, error) {
	repoPath := c.cfg.Git.TargetDir // Assuming TargetDir is the root of the cloned repos
	fullPath := filepath.Join(repoPath, filePath)

	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	contentBytes, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	contentString := string(contentBytes)

	a := &Content{
		ID:           utils.XXH3Hash(filePath), // Use filepath hash as ID
		Path:         filePath,
		Type:         "code",
		LastModified: fileInfo.ModTime().Format(time.RFC3339),
		URL:          "", // Git files don't have URLs
		Content:      contentString,
		Xxh3:         utils.XXH3Hash(contentString),
	}

	return a, nil
}
