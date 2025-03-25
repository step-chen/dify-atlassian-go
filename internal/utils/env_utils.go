package utils

import (
	"fmt"
	"os/exec"
	"strings"
)

const (
	MarkitdownRepo    = ".markitdown"
	MarkitdownRepoURL = "https://github.com/microsoft/markitdown.git"
)

type DockerUtils struct {
	markitdownImage bool
	pandocImage     bool
}

var dockerUtils DockerUtils = DockerUtils{markitdownImage: false, pandocImage: false}

// CheckImageExists checks if a docker image exists locally
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

// PullImage pulls a docker image if it doesn't exist
func PullImage(imageName string) error {
	cmd := exec.Command("docker", "pull", imageName)
	fmt.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))
	return cmd.Run()
}

// CheckRequiredDockerImages checks if both markitdown and pandoc docker images exist
func InitRequiredTools() {
	checkMarkitdownImage()
	checkPandoc()
}

func checkPandoc() {
	// Check if pandoc command exists
	if _, err := exec.LookPath("pandoc"); err != nil {
		fmt.Println("pandoc command not found:", err)
		return
	}

	// Get pandoc version
	cmd := exec.Command("pandoc", "--version")
	output, err := cmd.Output()
	if err != nil {
		fmt.Println("failed to get pandoc version:", err)
		return
	}

	// Print supported pandoc version
	fmt.Println("Supported pandoc version:")
	fmt.Println(string(output))
	dockerUtils.pandocImage = true
}

func checkMarkitdownImage() {
	if _, err := exec.LookPath("docker"); err != nil {
		return
	}

	if err := CheckImageExists(MarkitdownImage); err != nil {
		fmt.Println("failed to check markitdown image:", err)
		fmt.Println("build markitdown:latest image...")
		cmd := exec.Command("git", "clone", "--branch", "main", MarkitdownRepoURL, MarkitdownRepo)
		fmt.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))

		if err := cmd.Run(); err == nil {
			fmt.Println("successful to clone markitdown repository")

			cmd = exec.Command("docker", "build", "-t", MarkitdownImage, MarkitdownRepo)
			fmt.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))

			if err = cmd.Run(); err == nil {
				fmt.Println("successful to build markitdown:latest image")
				dockerUtils.markitdownImage = true
			} else {
				fmt.Println("failed to build markitdown:latest image:", err)
			}
		} else {
			fmt.Println("failed to clone markitdown repository:", err)
		}
	} else {
		fmt.Println("successful to check markitdown:latest image")
		dockerUtils.markitdownImage = true
	}
}
