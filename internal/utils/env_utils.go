package utils

import (
	"fmt"
	"log"
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
	log.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))
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
