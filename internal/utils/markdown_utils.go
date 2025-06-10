package utils

import (
	"bufio"
	"log"
	"strings"
)

// ExtractMarkdownTitleKeywords extracts keywords from the first markdown title in the given text.
// The function is now public (starts with an uppercase letter) to be accessible from other packages.
func ExtractMarkdownTitleKeywords(markdownContent string) string {
	scanner := bufio.NewScanner(strings.NewReader(markdownContent))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Check for markdown titles (#, ##, etc.)
		if strings.HasPrefix(line, "#") {
			// Find the first non-# character
			titleStart := 0
			for i := 0; i < len(line); i++ {
				if line[i] != '#' {
					titleStart = i
					break
				}
			}
			// Ensure there's a space after the hashes (standard markdown)
			if titleStart < len(line) && line[titleStart] == ' ' {
				titleText := strings.TrimSpace(line[titleStart+1:])
				// Split the title text into words (simple split by non-alphanumeric)
				return titleText
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error scanning markdown content for title: %v", err)
	}
	return ""
}
