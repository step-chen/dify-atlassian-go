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
	MarkitdownImage   = "markitdown:latest"

	ToolMarkitdown = 1 << iota
	ToolPandoc
	ToolGit
)

type DockerUtils struct {
	markitdownImage bool
	pandocImage     bool
}

var dockerUtils DockerUtils = DockerUtils{markitdownImage: false, pandocImage: false}

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

func PullImage(imageName string) error {
	cmd := exec.Command("docker", "pull", imageName)
	log.Println(cmd.Path + " " + strings.Join(cmd.Args[1:], " "))
	return cmd.Run()
}

func InitRequiredTools(tools int) {
	if tools&ToolMarkitdown != 0 {
		checkMarkitdownImage()
	}
	if tools&ToolPandoc != 0 {
		checkPandoc()
	}
	if tools&ToolGit != 0 {
		checkGit()
	}
}

func checkGit() {
	if _, err := exec.LookPath("git"); err != nil {
		log.Fatalln("git command not found:", err)
		return
	}

	cmd := exec.Command("git", "--version")
	output, err := cmd.Output()
	if err != nil {
		log.Fatalln("failed to get git version:", err)
		return
	}

	log.Println("support", strings.TrimSpace(string(output)))
}

func checkPandoc() {
	if _, err := exec.LookPath("pandoc"); err != nil {
		log.Println("pandoc command not found:", err)
		return
	}

	cmd := exec.Command("pandoc", "--version")
	output, err := cmd.Output()
	if err != nil {
		log.Println("failed to get pandoc version:", err)
		return
	}

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
