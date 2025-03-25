package utils

import (
	"strings"

	"github.com/microcosm-cc/bluemonday"
)

func SanitizeHTML(html string) string {
	p := bluemonday.NewPolicy()
	p.AllowElements("p", "br")
	sanitized := p.Sanitize(html)
	sanitized = strings.ReplaceAll(sanitized, "<p>", "\n")
	sanitized = strings.ReplaceAll(sanitized, "</p>", "")
	sanitized = strings.ReplaceAll(sanitized, "<br/>", "\n")
	return strings.TrimSpace(sanitized)
}
