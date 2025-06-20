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

	"golang.org/x/net/html"

	"github.com/PuerkitoBio/goquery"
	"github.com/microcosm-cc/bluemonday"
)

// Link represents a found link with its URL and title.
// Content is NOT stored here, it will be read on demand.
type Link struct {
	URL   string
	Title string
}

func SanitizeHTML(html string) string {
	p := bluemonday.NewPolicy()
	p.AllowElements("p", "br")
	sanitized := p.Sanitize(html)
	sanitized = strings.ReplaceAll(sanitized, "<p>", "\n")
	sanitized = strings.ReplaceAll(sanitized, "</p>", "")
	sanitized = strings.ReplaceAll(sanitized, "<br/>", "\n")
	return strings.TrimSpace(sanitized)
}

func ExtractKeywords(doc *goquery.Document, keywordsBlocks []string) []string {
	if doc == nil || len(keywordsBlocks) == 0 {
		return []string{}
	}

	// targetHeadingTexts stores lowercased heading texts (e.g., "Introduction")
	// from which to extract keywords.
	targetHeadingTexts := make(map[string]bool)
	for _, headingText := range keywordsBlocks {
		targetHeadingTexts[strings.ToLower(headingText)] = true
	}

	var keywords []string

	doc.Find("h1, h2, h3, h4, h5, h6").Each(func(i int, s *goquery.Selection) {
		currentHeadingText := strings.ToLower(strings.TrimSpace(s.Text()))

		if targetHeadingTexts[currentHeadingText] {
			var blockContent strings.Builder
			s.NextAll().EachWithBreak(func(j int, siblingNodeSel *goquery.Selection) bool {
				node := siblingNodeSel.Get(0)
				if node != nil && node.Type == html.ElementNode {
					nodeName := strings.ToLower(node.Data)
					if len(nodeName) == 2 && nodeName[0] == 'h' && (nodeName[1] >= '1' && nodeName[1] <= '6') {
						// It's a subsequent heading, so stop collecting content for the current block.
						return false // Break the EachWithBreak loop.
					}
				}

				blockContent.WriteString(siblingNodeSel.Text())
				blockContent.WriteString(" ") // Add a space to separate text from different elements.
				return true                   // Continue to the next sibling.
			})

			words := SplitWords(blockContent.String())
			keywords = append(keywords, words...)
		}
	})

	return RemoveDuplicates(keywords)
}

// SplitWords splits text into individual words
func SplitWords(text string) []string {
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	var cleanedWords []string
	for _, word := range words {
		cleaned := strings.TrimSpace(word)
		if cleaned != "" {
			cleanedWords = append(cleanedWords, cleaned)
		}
	}

	return cleanedWords
}

// RemoveDuplicates removes duplicate strings from a slice.
func RemoveDuplicates(strs []string) []string {
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

// combineContentWithReferences takes original HTML, reads content of local file links,
// and combines them into a single markdown string.
func combineContentWithReferences(originalHTMLContent []byte, links []Link, unsupportedBlocks []string) (string, error) {
	var sb strings.Builder
	sb.Write(originalHTMLContent)

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
			fmt.Printf("Warning: Failed to read content for linked file '%s': %v. Skipping its inclusion.\n", link.URL, err)
			continue
		}

		// Normalize whitespace for linked file content
		linkedFileContent = normalizeHTMLWhitespace(linkedFileContent)

		// Only parse HTML and remove blocks if there are unsupported blocks defined.
		// This assumes the linked file is HTML if unsupportedBlocks is not empty.
		if len(unsupportedBlockSet) > 0 {
			doc, parseErr := goquery.NewDocumentFromReader(bytes.NewReader(linkedFileContent))
			if parseErr != nil {
				fmt.Printf("Warning: Failed to parse HTML for linked file '%s': %v. Appending raw content.\n", link.URL, parseErr)
			} else {
				removeUnsupportedBlocks(doc, unsupportedBlocks) // Use the original slice for the helper
				var buf bytes.Buffer
				if htmlErr := goquery.Render(&buf, doc.Selection); htmlErr != nil {
					fmt.Printf("Warning: Failed to render modified HTML for linked file '%s': %v. Appending raw content.\n", link.URL, htmlErr)
				} else {
					linkedFileContent = buf.Bytes()
				}
			}
		}

		sb.WriteString(fmt.Sprintf("<h2><a href=\"%s\">%s</a></h2>\n", link.URL, link.Title))
		sb.WriteString("<pre><code>\n")
		sb.Write(linkedFileContent)
		sb.WriteString("\n</code></pre>\n")
		sb.WriteString("<hr/>\n") // Add a separator for clarity between combined files.
	}

	content, err := ConvertContent2Markdown(sb.String(), "text/html", "", ConversionMethodCommon)
	if err != nil {
		return "", err
	} else {
		return content, nil
	}
}

// AppendHtmlRef processes an HTML document: extracts local file references, normalizes them,
// combines their content with the original, and converts to markdown.
// It performs validation and deduplication and does NOT read linked file content into the Link struct directly,
// that happens in combineContentWithReferences.
func AppendHtmlRef(doc *goquery.Document, unsupportedFilters []string, unsupportedBlocks []string) (string, error) {

	// Apply block removal to the original document itself
	removeUnsupportedBlocks(doc, unsupportedBlocks)

	var links []Link
	seenURLs := make(map[string]struct{})
	baseDir := "" // Base directory for resolving relative paths, derived from doc.Url.
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
			return
		}

		absoluteLocalPath, isValidLocalRef := getAbsoluteLocalPath(href, baseDir)
		if !isValidLocalRef {
			// Not a valid local reference (e.g., external URL, mailto), replace <a> tag with its text.
			s.ReplaceWithHtml(s.Text())
			return
		}

		if _, found := seenURLs[absoluteLocalPath]; found {
			// Already processed this link, replace <a> tag with its text to avoid duplication.
			s.ReplaceWithHtml(s.Text())
			return
		}

		// Check if the file exists and is accessible using os.Stat
		if _, err := os.Stat(absoluteLocalPath); err != nil {
			// File doesn't exist or is inaccessible, replace <a> tag with its text.
			s.ReplaceWithHtml(s.Text())
			return
		}

		// Check if the path contains any unsupported filter
		if containsUnsupportedFilter(absoluteLocalPath, unsupportedFilterSet) {
			// Path matches an unsupported filter, replace <a> tag with its text.
			s.ReplaceWithHtml(s.Text())
			return
		}

		title := extractLinkTitle(s, absoluteLocalPath)
		links = append(links, Link{
			URL:   absoluteLocalPath,
			Title: title,
		})
		seenURLs[absoluteLocalPath] = struct{}{}

		// After extracting the link, convert it to plain text in the original HTML
		s.ReplaceWithHtml(s.Text())
	})

	// Render the modified original HTML content before passing it to combineContentWithReferences
	var modifiedOriginalHTML bytes.Buffer
	if err := goquery.Render(&modifiedOriginalHTML, doc.Selection); err != nil {
		return "", fmt.Errorf("failed to render modified original HTML: %w", err)
	}

	return combineContentWithReferences(modifiedOriginalHTML.Bytes(), links, unsupportedBlocks)
}

func extractLinkTitle(s *goquery.Selection, fallbackPath string) string { // extracts the link title
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

func containsUnsupportedFilter(path string, filterSet map[string]struct{}) bool { // checks if a path contains any unsupported filter
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
		return "", false
	}

	if u.IsAbs() && u.Scheme != "" && u.Scheme != "file" {
		return "", false // It's an absolute URL with a scheme other than "file" (e.g., http, mailto)
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
		return "", false
	}

	return cleanPath, true
}

// removeUnsupportedBlocks removes specified heading and content blocks from a goquery document.
func removeUnsupportedBlocks(doc *goquery.Document, unsupportedBlocks []string) {
	unsupportedBlockSet := make(map[string]struct{})
	if len(unsupportedBlocks) == 0 {
		return
	}
	for _, blockTitle := range unsupportedBlocks {
		unsupportedBlockSet[blockTitle] = struct{}{}
	}

	doc.Find("h1, h2, h3, h4, h5, h6").Each(func(i int, s *goquery.Selection) {
		headingText := strings.TrimSpace(s.Text())
		if _, found := unsupportedBlockSet[headingText]; found {
			// Get all siblings *after* the current heading
			nextSiblings := s.NextAll()

			s.Remove()

			// Iterate through the collected next siblings and remove them until another heading is encountered.
			for _, siblingNode := range nextSiblings.Nodes {
				currentSelection := goquery.NewDocumentFromNode(siblingNode)

				isHeading := false
				if siblingNode.Type == html.ElementNode {
					tagName := strings.ToLower(siblingNode.Data)
					if len(tagName) == 2 && strings.HasPrefix(tagName, "h") && tagName[1] >= '1' && tagName[1] <= '6' {
						isHeading = true
					}
				}

				if isHeading {
					break
				}
				currentSelection.Remove()
			}
		}
	})
}

// normalizeHTMLWhitespace removes multiple spaces, tabs, and empty lines from HTML content.
// It tries to be careful not to affect content within <pre> or <code> tags.
func normalizeHTMLWhitespace(htmlContent []byte) []byte {
	contentStr := string(htmlContent)

	// Regex to find <pre>/<code> blocks or sequences of whitespace.
	// Preserves content within <pre>/<code>, replaces other multiple whitespace with a single space.
	re := regexp.MustCompile(`(?s)(?U)(?P<pre_code><(?:pre|code)>.*<\/(?:pre|code)>)|[\t ]+`)
	contentStr = re.ReplaceAllStringFunc(contentStr, func(match string) string {
		if strings.HasPrefix(match, "<pre>") || strings.HasPrefix(match, "<code>") {
			return match
		}
		if strings.TrimSpace(match) == "" {
			return " "
		}
		return match
	})

	// Remove empty lines (lines containing only whitespace, or now just a single space from previous step).
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
			cleanedLines = append(cleanedLines, line)
		} else if trimmedLine != "" {
			cleanedLines = append(cleanedLines, trimmedLine)
		}
	}
	contentStr = strings.Join(cleanedLines, "\n")

	return []byte(contentStr)
}
