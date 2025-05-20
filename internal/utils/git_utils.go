package utils

import (
	"fmt"
	"os/exec"
	"strings"
)

// GitClone clones a git repository to the specified directory
func GitClone(repoURL, targetDir, username, password string) error {
	// Construct authenticated URL
	authURL := fmt.Sprintf("https://%s:%s@%s",
		username,
		password,
		strings.TrimPrefix(repoURL, "https://"))

	cmd := exec.Command("git", "clone", authURL, targetDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git clone failed: %s\n%s", err, string(output))
	}
	return nil
}

// IsGitRepo checks if a directory is a git repository
func IsGitRepo(dir string) bool {
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--git-dir")
	return cmd.Run() == nil
}

// GetGitRoot returns the root directory of a git repository
func GetGitRoot(dir string) (string, error) {
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get git root: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

// GitPull updates an existing git repository
func GitPull(repoPath string) error {
	cmd := exec.Command("git", "-C", repoPath, "pull")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git pull failed: %s\n%s", err, string(output))
	}
	return nil
}
