package utils

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
)

const (
	MarkitdownRepo    = ".markitdown"                                 // Local directory for markitdown repository
	MarkitdownRepoURL = "https://github.com/microsoft/markitdown.git" // Markitdown repository URL
)

// DockerUtils tracks required Docker image availability
type DockerUtils struct {
	markitdownImage bool // Indicates if markitdown image is available
	pandocImage     bool // Indicates if pandoc image is available
}

var dockerUtils DockerUtils = DockerUtils{markitdownImage: false, pandocImage: false}

// CheckImageExists verifies if a Docker image is available locally
// imageName: Name of the Docker image to check
// Returns error if image not found or check fails
func CheckImageExists(imageName string) error {
	cmd := exec.Command("docker", "images", "-q", imageName)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to check docker images: %w", err)
	}

	if len(output) == 0 {
		return fmt.Errorf("docker image %s not found", imageName)
	}

	return nil
}

// PullImage downloads a Docker image if not present locally
// imageName: Name of the Docker image to pull
// Returns error if pull operation fails
func PullImage(imageName string) error {
	cmd := exec.Command("docker", "pull", imageName)
	log.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))
	return cmd.Run()
}

// InitRequiredTools verifies and initializes required tools
// Checks for pandoc installation and markitdown Docker image
func InitRequiredTools() {
	checkMarkitdownImage()
	checkPandoc()
}

// checkPandoc verifies pandoc installation and version
func checkPandoc() {
	// Check if pandoc command exists
	if _, err := exec.LookPath("pandoc"); err != nil {
		log.Println("pandoc command not found:", err)
		return
	}

	// Get pandoc version
	cmd := exec.Command("pandoc", "--version")
	output, err := cmd.Output()
	if err != nil {
		log.Println("failed to get pandoc version:", err)
		return
	}

	// Print supported pandoc version
	// log.Println("Supported pandoc version:")
	log.Println("support", strings.Split(string(output), "\n")[0])
	dockerUtils.pandocImage = true
}

// checkMarkitdownImage verifies and builds markitdown Docker image if needed
func checkMarkitdownImage() {
	if _, err := exec.LookPath("docker"); err != nil {
		return
	}

	if err := CheckImageExists(MarkitdownImage); err != nil {
		log.Println("failed to check markitdown image:", err)
		log.Println("build markitdown:latest image...")
		cmd := exec.Command("git", "clone", "--branch", "main", MarkitdownRepoURL, MarkitdownRepo)
		log.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))

		if err := cmd.Run(); err == nil {
			log.Println("successful to clone markitdown repository")

			cmd = exec.Command("docker", "build", "-t", MarkitdownImage, MarkitdownRepo)
			log.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))

			if err = cmd.Run(); err == nil {
				log.Println("successful to build markitdown:latest image")
				dockerUtils.markitdownImage = true
			} else {
				log.Println("failed to build markitdown:latest image:", err)
			}
		} else {
			log.Println("failed to clone markitdown repository:", err)
		}
	} else {
		log.Println("successful to check markitdown:latest image")
		dockerUtils.markitdownImage = true
	}
}
