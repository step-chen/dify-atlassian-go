package bitbucket

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"

	CFG "github.com/step-chen/dify-atlassian-go/internal/config/bitbucket"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

func NewClient(baseURL, apiKey string, allowedTypes map[string]utils.ConversionMethod, unsupportedTypes map[string]bool, separator, parentMode string) (*Client, error) {
	if baseURL == "" {
		return nil, fmt.Errorf("baseURL is required")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("apiKey is required")
	}

	return &Client{
		baseURL:          strings.TrimSuffix(baseURL, "/"),
		apiKey:           apiKey,
		client:           &http.Client{},
		allowedTypes:     allowedTypes,
		unsupportedTypes: unsupportedTypes,
		separator:        separator,
		parentMode:       parentMode,
	}, nil
}

func (c *Client) prepareQuery(req *http.Request, limit string, start string) *http.Request {
	q := req.URL.Query()
	if limit != "" {
		q.Add("limit", limit)
	}
	if start != "" {
		q.Add("start", start)
	}

	req.URL.RawQuery = q.Encode()

	return utils.AddBearerAuthHeader(c.apiKey, req)
}

// buildBrowseURL constructs the full URL string for browsing repository contents at a specific path.
func (c *Client) buildURL(repoInfo *CFG.RepoInfo, currentPath, contentType string) (string, error) {
	// Construct API request URL using net/url for robustness
	parsedBaseURL, err := url.Parse(c.baseURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse base URL '%s': %v", c.baseURL, err)
	}

	// Build the API path.
	// Note: repoInfo.Brantch contains a typo ("Brantch" instead of "Branch") originating from the config struct.
	// It's recommended to fix the typo in CFG.RepoInfo and the corresponding YAML field.
	apiPathSegments := []string{
		"projects",
		repoInfo.Project,
		"repos",
		repoInfo.ReposSlug,
		contentType,
		repoInfo.Brantch, // Typo from config
		"src",
	}
	// Append currentPath to the API path segments. path.Join handles empty currentPath correctly.
	requestSpecificPath := path.Join(append(apiPathSegments, currentPath)...)

	finalURL := *parsedBaseURL                                         // Make a copy
	finalURL.Path = path.Join(parsedBaseURL.Path, requestSpecificPath) // Safely join with base URL's path

	return finalURL.String(), nil
}

// GetContentsList recursively retrieves all files from the configured directory and its subdirectories,
// applying the file filter specified in the configuration.
func (c *Client) GetContentsList(repo *CFG.RepoInfo) (map[string]batchpool.Operation, error) {
	// Get the root directory from the config
	rootPath := repo.Directory
	// Start recursive processing from the root directory
	contents := make(map[string]batchpool.Operation)
	err := c.processDirectory(rootPath, repo, contents)
	if err != nil {
		return nil, err
	}

	return contents, nil
}

// processDirectory handles recursive directory processing for Bitbucket
func (c *Client) processDirectory(currentPath string, repoInfo *CFG.RepoInfo, contents map[string]batchpool.Operation) error {
	start := "0"   // Initial start for pagination
	limit := "100" // Sensible default limit, consider making this configurable

	for {
		browseAPIURL, err := c.buildURL(repoInfo, currentPath, "browse")
		if err != nil {
			return err // Error from buildBrowseURL is already descriptive
		}

		// Create and prepare HTTP request
		req, err := http.NewRequest("GET", browseAPIURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request for path '%s' (URL: %s): %v", currentPath, browseAPIURL, err)
		}

		// Correctly call prepareQuery, passing repoInfo.
		// Note: The 'repoInfo' and 'expand' parameters are currently unused in the prepareQuery implementation.
		req = c.prepareQuery(req, limit, start)

		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to fetch contents for path '%s' (start: %s): %v", currentPath, start, err)
		}
		defer resp.Body.Close()

		var result BrowseResponse
		//var res1 interface{}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode response for path '%s' (start: %s): %v", currentPath, start, err)
		}

		// Process each child item
		for _, child := range result.Children.Values {
			fullItemPath := path.Join(currentPath, child.Path.Name)

			if child.Type == "DIRECTORY" {
				if err := c.processDirectory(fullItemPath, repoInfo, contents); err != nil {
					return err
				}
			} else if child.Type == "FILE" {
				if matched, err := utils.MatchesFilters(repoInfo.Filter, fullItemPath); err != nil {
					log.Printf("error matching pattern for file %s: %v", fullItemPath, err)
					continue
				} else if !matched {
					continue
				} else {
					contents[fullItemPath] = batchpool.Operation{
						Action: batchpool.ActionCreate, // Assuming initial sync means create
						Type:   batchpool.GitFile,
						Hash:   child.ContentId,
					}
				}
			}
		}

		if result.Children.IsLastPage || result.Children.NextPageStart == nil {
			break // Exit loop if it's the last page or NextPageStart is not provided
		}
		start = fmt.Sprintf("%d", *result.Children.NextPageStart)
	}
	return nil
}

func (c *Client) GetContent(repoInfo *CFG.RepoInfo, currentPath string) (*Content, error) {
	rawAPIURL, err := c.buildURL(repoInfo, currentPath, "raw")
	if err != nil {
		return nil, fmt.Errorf("failed to build raw content URL for path '%s': %w", currentPath, err)
	}

	req, err := http.NewRequest("GET", rawAPIURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for raw content at '%s': %w", rawAPIURL, err)
	}

	req = utils.AddBearerAuthHeader(c.apiKey, req)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch raw content from '%s': %w", rawAPIURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// TODO: Consider reading a small part of the body for detailed error messages from Bitbucket
		return nil, fmt.Errorf("failed to fetch raw content from '%s', status: %s", rawAPIURL, resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body from '%s': %w", rawAPIURL, err)
	}

	// Calculate XXH3 hash
	xxh3String := utils.XXH3FromBytes(bodyBytes)

	// Determine MediaType
	mediaType := utils.GetMIMEType(currentPath)
	if mediaType == "" {
		mediaType = "application/octet-stream" // Default fallback
	}

	content := &Content{
		ID:        currentPath, // Use currentPath as a unique identifier
		Type:      "bitbucket", // Source type
		Title:     path.Base(currentPath),
		URL:       rawAPIURL,
		RAW:       bodyBytes,
		MediaType: mediaType,
		Xxh3:      xxh3String,
	}

	return content, nil
}
