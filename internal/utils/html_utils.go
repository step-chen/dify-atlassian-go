package utils

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

// Link represents a found link with its URL and title.
// Content is NOT stored here, it will be read on demand.
type Link struct {
	URL   string
	Title string
}

func ExtractKeywords(doc *goquery.Document, keywordsBlocks []string) []string {
	// 输入验证
	if doc == nil || len(keywordsBlocks) == 0 {
		return []string{}
	}

	// 创建允许标签的集合
	allowedTags := make(map[string]bool)
	for _, tag := range keywordsBlocks {
		allowedTags[strings.ToLower(tag)] = true
	}

	var keywords []string

	// 遍历所有元素节点
	doc.Find("*").Each(func(i int, s *goquery.Selection) {
		// 获取当前标签名
		tagName := strings.ToLower(s.Nodes[0].Data)

		// 如果是script或style标签，跳过处理
		if tagName == "script" || tagName == "style" {
			return
		}

		// 如果当前标签在允许的标签列表中
		if allowedTags[tagName] {
			// 获取文本内容并提取关键词
			text := s.Text()
			words := splitWords(text)
			keywords = append(keywords, words...)
		}
	})

	return deduplicate(keywords)
}

// splitWords splits text into individual words
func splitWords(text string) []string {
	// Split on non-alphanumeric characters and convert to lowercase
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	var cleanedWords []string
	for _, word := range words {
		cleaned := strings.ToLower(strings.TrimSpace(word))
		if cleaned != "" {
			cleanedWords = append(cleanedWords, cleaned)
		}
	}

	return cleanedWords
}

// deduplicate removes duplicate keywords
func deduplicate(strs []string) []string {
	seen := make(map[string]bool)
	var result []string

	for _, s := range strs {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}

	return result
}

// combineContentWithReferences takes the original HTML content and the extracted links.
// It reads the content of each referenced local file within this function.
func combineContentWithReferences(originalHTMLContent []byte, links []Link, unsupportedBlocks []string) (string, error) {
	var sb strings.Builder
	sb.Write(originalHTMLContent) // Start with the original HTML content

	sb.WriteString("\n\n\n")
	sb.WriteString("<h1>Combined Local References Content:</h1>\n")

	// Pre-process unsupported blocks into a set for efficient lookups
	unsupportedBlockSet := make(map[string]struct{})
	if len(unsupportedBlocks) > 0 {
		for _, blockTitle := range unsupportedBlocks {
			unsupportedBlockSet[blockTitle] = struct{}{}
		}
	}

	for _, link := range links {
		linkedFileContent, err := os.ReadFile(link.URL) // link.URL is already an absolute path
		if err != nil {
			// Log the error but continue processing other links.
			fmt.Printf("Warning: Failed to read content for linked file '%s': %v. Skipping content inclusion for this link.\n", link.URL, err)
			continue // Skip this link if its content cannot be read
		}

		// Normalize whitespace for linked file content
		linkedFileContent = normalizeHTMLWhitespace(linkedFileContent)

		// Only parse HTML and remove blocks if there are unsupported blocks defined.
		// This assumes the linked file is HTML if unsupportedBlocks is not empty.
		if len(unsupportedBlockSet) > 0 {
			doc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(linkedFileContent))
			if parseErr != nil {
				fmt.Printf("Warning: Failed to parse HTML for linked file '%s': %v. Appending raw content.\n", link.URL, parseErr)
				// Fallback to appending raw content if parsing fails
			} else {
				removeUnsupportedBlocks(doc, unsupportedBlocks) // Use the original slice for the helper
				var buf bytes.Buffer
				if htmlErr := goquery.Render(&buf, doc.Selection); htmlErr != nil {
					fmt.Printf("Warning: Failed to render modified HTML for linked file '%s': %v. Appending raw content.\n", link.URL, htmlErr)
				} else {
					linkedFileContent = buf.Bytes() // Use the modified content
				}
			}
		}

		sb.WriteString(fmt.Sprintf("<h2><a href=\"%s\">%s</a></h2>\n", link.URL, link.Title))
		sb.WriteString("<pre><code>\n") // Use <pre> and <code> for code-like content
		sb.Write(linkedFileContent)     // Append the actual content of the linked file
		sb.WriteString("\n</code></pre>\n")
		sb.WriteString("<hr/>\n") // Separator for clarity
	}

	content, err := ConvertContent2Markdown(sb.String(), "text/html", "", ConversionMethodCommon)
	if err != nil {
		return "", err
	} else {
		return content, nil
	}
}

// AppendHtmlRef processes an HTML document, extracts and normalizes unique local file references (URLs).
// It performs validation and deduplication but does NOT read the content of linked files into Link struct.
func AppendHtmlRef(doc *goquery.Document, unsupportedFilters []string, unsupportedBlocks []string) (string, error) {

	// Apply block removal to the original document itself
	removeUnsupportedBlocks(doc, unsupportedBlocks)

	var links []Link
	seenURLs := make(map[string]struct{}) // Use a map as a set for deduplication
	baseDir := ""
	if doc.Url != nil {
		baseDir = filepath.Dir(doc.Url.Path)
	}

	// Convert unsupported filters to a Set for efficient lookups
	unsupportedFilterSet := make(map[string]struct{})
	for _, filter := range unsupportedFilters {
		unsupportedFilterSet[filter] = struct{}{}
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		href, exists := s.Attr("href")
		if !exists || href == "" {
			// If no href, we might still want to process its text as plain text
			// No change needed for this specific `<a>` tag as it's not a valid link for extraction.
			return // Skip if no href attribute or it's empty
		}

		absoluteLocalPath, isValidLocalRef := getAbsoluteLocalPath(href, baseDir)
		if !isValidLocalRef {
			// If not a valid local reference, we should remove the link and keep the text.
			s.ReplaceWithHtml(s.Text()) // Replace the <a> tag with its text content
			return                      // Skip if not a valid local reference
		}

		if _, found := seenURLs[absoluteLocalPath]; found {
			// If already seen, remove the link and keep the text.
			s.ReplaceWithHtml(s.Text()) // Replace the <a> tag with its text content
			return                      // Skip if already seen (deduplication)
		}

		// Check if the file exists and is accessible using os.Stat
		if _, err := os.Stat(absoluteLocalPath); err != nil {
			// File doesn't exist or inaccessible, remove the link and keep the text.
			s.ReplaceWithHtml(s.Text()) // Replace the <a> tag with its text content
			return                      // File doesn't exist or inaccessible, skip
		}

		// Check if the path contains any unsupported filter
		if containsUnsupportedFilter(absoluteLocalPath, unsupportedFilterSet) {
			// Contains an unsupported filter, remove the link and keep the text.
			s.ReplaceWithHtml(s.Text()) // Replace the <a> tag with its text content
			return                      // Contains an unsupported filter, skip
		}

		// Extract the link title
		title := extractLinkTitle(s, absoluteLocalPath)

		links = append(links, Link{
			URL:   absoluteLocalPath,
			Title: title,
		})
		seenURLs[absoluteLocalPath] = struct{}{}

		// After extracting the link, convert it to plain text in the original HTML
		s.ReplaceWithHtml(s.Text()) // Replace the <a> tag with its text content
	})

	// Render the modified original HTML content before passing it to combineContentWithReferences
	var modifiedOriginalHTML bytes.Buffer
	if err := goquery.Render(&modifiedOriginalHTML, doc.Selection); err != nil {
		return "", fmt.Errorf("failed to render modified original HTML: %w", err)
	}

	return combineContentWithReferences(modifiedOriginalHTML.Bytes(), links, unsupportedBlocks)
}

// Helper function: extracts the link title
func extractLinkTitle(s *goquery.Selection, fallbackPath string) string {
	title := strings.TrimSpace(s.Text())
	if title == "" {
		img := s.Find("img").First()
		if img.Length() > 0 {
			alt, altExists := img.Attr("alt")
			if altExists && alt != "" {
				title = alt
			} else {
				title = fallbackPath
			}
		} else {
			title = fallbackPath
		}
	}
	return title
}

// Helper function: checks if a path contains any unsupported filter
func containsUnsupportedFilter(path string, filterSet map[string]struct{}) bool {
	for filter := range filterSet {
		if strings.Contains(path, filter) {
			return true
		}
	}
	return false
}

// getAbsoluteLocalPath attempts to resolve a given href to an absolute local file path.
// It returns the absolute path and a boolean indicating if it's a valid local reference.
func getAbsoluteLocalPath(href string, baseDir string) (string, bool) {
	u, err := url.Parse(href)
	if err != nil {
		return "", false // Invalid URL format
	}

	if u.IsAbs() && u.Scheme != "" && u.Scheme != "file" {
		return "", false // External URL (http, https, ftp, etc.)
	}

	var resolvedPath string
	if u.Scheme == "file" {
		resolvedPath = filepath.FromSlash(u.Path)
	} else if filepath.IsAbs(href) {
		resolvedPath = href
	} else {
		resolvedPath = filepath.Join(baseDir, href)
	}

	cleanPath := filepath.Clean(resolvedPath)

	if !filepath.IsAbs(cleanPath) {
		return "", false // Path did not resolve to a clean absolute path
	}

	return cleanPath, true
}

// removeUnsupportedBlocks removes heading and content blocks from a goquery document
// based on a list of unsupported block titles.
func removeUnsupportedBlocks(doc *goquery.Document, unsupportedBlocks []string) {
	unsupportedBlockSet := make(map[string]struct{})
	if len(unsupportedBlocks) == 0 {
		return // No unsupported blocks to remove
	}
	for _, blockTitle := range unsupportedBlocks {
		unsupportedBlockSet[blockTitle] = struct{}{}
	}

	// Iterate over all heading elements (h1-h6)
	doc.Find("h1, h2, h3, h4, h5, h6").Each(func(i int, s *goquery.Selection) {
		headingText := strings.TrimSpace(s.Text())
		if _, found := unsupportedBlockSet[headingText]; found {
			// Get all siblings *after* the current heading
			nextSiblings := s.NextAll()

			// Remove the heading itself first
			s.Remove()

			// Iterate through the collected next siblings and remove them
			// until another heading is encountered.
			for _, siblingNode := range nextSiblings.Nodes {
				currentSelection := goquery.NewDocumentFromNode(siblingNode)

				isHeading := false
				if siblingNode.Type == 1 { // ElementNode
					tagName := strings.ToLower(siblingNode.Data)
					if len(tagName) == 2 && strings.HasPrefix(tagName, "h") && tagName[1] >= '1' && tagName[1] <= '6' {
						isHeading = true
					}
				}

				if isHeading {
					break // Stop if we encounter another heading
				}
				currentSelection.Remove() // Remove the content block
			}
		}
	})
}

// normalizeHTMLWhitespace removes multiple spaces, tabs, and empty lines from HTML content.
// It tries to be careful not to affect content within <pre> or <code> tags.
func normalizeHTMLWhitespace(htmlContent []byte) []byte {
	// Convert to string for regex operations
	contentStr := string(htmlContent)

	// Remove extra spaces and tabs
	// This regex targets one or more whitespace characters (space, tab, newline, carriage return)
	// that are NOT within <pre> or <code> tags.
	// It's a simplified approach and might need refinement for complex cases.
	re := regexp.MustCompile(`(?s)(?U)(?P<pre_code><(?:pre|code)>.*<\/(?:pre|code)>)|[\t ]+`)
	contentStr = re.ReplaceAllStringFunc(contentStr, func(match string) string {
		if strings.HasPrefix(match, "<pre>") || strings.HasPrefix(match, "<code>") {
			return match // Don't modify content inside <pre> or <code> tags
		}
		if strings.TrimSpace(match) == "" { // If it's just whitespace
			return " " // Replace with a single space
		}
		return match // Keep other matches as is
	})

	// Remove empty lines (lines containing only whitespace)
	// This will remove lines that are entirely empty or contain only spaces/tabs.
	reEmptyLines := regexp.MustCompile(`(?m)^\s*\n`)
	contentStr = reEmptyLines.ReplaceAllString(contentStr, "")

	// Remove leading/trailing whitespace from each line that is not inside <pre> or <code>
	// This is tricky with regex if you want to preserve line breaks, so let's do it line by line.
	lines := strings.Split(contentStr, "\n")
	var cleanedLines []string
	inPreCodeBlock := false
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if strings.Contains(line, "<pre>") || strings.Contains(line, "<code>") {
			inPreCodeBlock = true
		} else if strings.Contains(line, "</pre>") || strings.Contains(line, "</code>") {
			inPreCodeBlock = false
		}

		if inPreCodeBlock {
			cleanedLines = append(cleanedLines, line) // Keep pre/code lines as they are
		} else if trimmedLine != "" {
			cleanedLines = append(cleanedLines, trimmedLine) // Add non-empty trimmed lines
		}
	}
	contentStr = strings.Join(cleanedLines, "\n")

	return []byte(contentStr)
}
