package bitbucket

import (
	"net/http"

	P "github.com/step-chen/dify-atlassian-go/internal/content_parser"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

type Client struct {
	baseURL          string
	apiKey           string
	client           *http.Client
	allowedTypes     map[string]utils.ConversionMethod
	unsupportedTypes map[string]bool
	separator        string
	parentMode       string
}

type Content struct {
	ID        string            `json:"-"`
	Type      string            `json:"type"`
	Title     string            `json:"title"`
	URL       string            `json:"url"`
	RAW       []byte            `json:"raw"`
	Contents  []P.ParsedContent `json:"-"`
	MediaType string            `json:"mediaType"`
	Xxh3      string            `json:"xxh3"`
}

// PathInfo represents the path details in Bitbucket API responses.
type PathInfo struct {
	Components []string `json:"components"`
	Parent     string   `json:"parent"`              // Parent path string
	Name       string   `json:"name"`                // Name of the file or directory
	Extension  string   `json:"extension,omitempty"` // File extension, if applicable
	ToString   string   `json:"toString"`            // String representation of the path
}

// ChildItem represents a file or directory item within a Bitbucket directory listing.
type ChildItem struct {
	Path      PathInfo `json:"path"`                // Path details of the item
	Node      string   `json:"node,omitempty"`      // Git commit hash or node ID
	ContentId string   `json:"contentId,omitempty"` // Git
	Type      string   `json:"type"`                // Type of the item, e.g., "FILE", "DIRECTORY"
}

// BrowseChildren represents the 'children' part of the browse API response,
// including pagination details.
type BrowseChildren struct {
	Size          int         `json:"size"`
	Limit         int         `json:"limit"`
	IsLastPage    bool        `json:"isLastPage"`
	Values        []ChildItem `json:"values"`
	Start         int         `json:"start"`
	NextPageStart *int        `json:"nextPageStart,omitempty"` // Use pointer for optional field that could be absent
}

// BrowseResponse is the structure for the Bitbucket API response when browsing
// repository contents.
type BrowseResponse struct {
	Path     PathInfo       `json:"path"`     // Path details of the browsed directory
	Revision string         `json:"revision"` // Revision of the browsed content (e.g., branch name)
	Children BrowseChildren `json:"children"` // Child items (files and directories)
}
