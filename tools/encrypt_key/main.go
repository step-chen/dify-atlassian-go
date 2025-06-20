package main

import (
	"fmt"
	"log"
	"os"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run tools/encrypt_key/main.go <api-key>")
	}

	apiKey := os.Args[1]
	encryptedKey, err := config.Encrypt(apiKey)
	if err != nil {
		log.Fatalf("Failed to encrypt API key: %v", err)
	}

	fmt.Println("Encrypted API Key:")
	fmt.Println(encryptedKey)
	fmt.Println("\nPlease update your config.yaml with this encrypted key")
}
