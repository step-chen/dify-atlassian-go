package confluence

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

// NewClient creates a new Confluence client
func NewClient(baseURL, apiKey string, allowedTypes, unsupportedTypes map[string]bool) (*Client, error) {
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
	}, nil
}

// DownloadAttachment downloads an attachment from the provided URL
func (c *Client) DownloadAttachment(url string, fileName string, mediaType string) (showPath string, tmpPath string, err error) {
	return utils.PrepareAttachmentFile(url, c.apiKey, fileName, mediaType, c.allowedTypes)
}

func (c *Client) GetBaseURL() string {
	return c.baseURL
}

func (c *Client) prepareHeader(req *http.Request) *http.Request {
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	return req
}

func (c *Client) preparePageQuery(req *http.Request, limit string, start string, expand []string) *http.Request {
	q := req.URL.Query()
	if limit != "" {
		q.Add("limit", limit)
	}
	if start != "" {
		q.Add("start", start)
	}
	if len(expand) > 0 {
		q.Add("expand", strings.Join(expand, ","))
	}
	req.URL.RawQuery = q.Encode()

	c.prepareHeader(req)

	return req
}

func (c *Client) GetSpaceContentsList(spaceKey string) (contents map[string]ContentOperation, err error) {
	nextPageURL := c.baseURL + "/rest/api/space/" + spaceKey + "/content/page"

	contents = make(map[string]ContentOperation)
	for nextPageURL != "" {
		req, err := http.NewRequest("GET", nextPageURL, nil)
		if err != nil {
			return contents, fmt.Errorf("failed to create request: %v", err)
		}

		if req.URL.Path == "/rest/api/space/"+spaceKey+"/content/page" {
			req = c.preparePageQuery(req, "100", "0", []string{"version.when", "children.attachment.version.when"})
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return contents, fmt.Errorf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return contents, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var result RawContentOperation
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return contents, fmt.Errorf("failed to unmarshal response: %v", err)
		}

		for _, content := range result.Results {
			// Add page content
			contents[content.ID] = ContentOperation{
				Action:           0,
				LastModifiedDate: content.Version.When,
				Type:             0,
			}

			// Add attachments
			for _, att := range content.Children.RawAttachment.Results {
				if !c.unsupportedTypes[att.Metadata.MediaType] {
					contents[att.ID] = ContentOperation{
						Action:           0,
						LastModifiedDate: att.Version.When,
						Type:             1,
						MediaType:        att.Metadata.MediaType,
					}
				}
			}
		}

		if result.Links.Next != "" {
			nextPageURL = c.baseURL + result.Links.Next
		} else {
			nextPageURL = ""
		}
	}

	return contents, nil
}

// []string{"body.view", "version", "history", "children.attachment.version", "children.comment", "metadata.labels"
// GetContent retrieves a single content by ID
func (c *Client) GetContent(contentID string) (*Content, error) {
	url := c.baseURL + "/rest/api/content/" + contentID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req = c.preparePageQuery(req, "", "", []string{"body.view", "version", "history", "metadata.labels"})

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var rawContent RawContentDetail

	if err := json.NewDecoder(resp.Body).Decode(&rawContent); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	var contentKeywords []string
	for _, label := range rawContent.Metadata.Labels.Results {
		contentKeywords = append(contentKeywords, label.Name)
	}

	content := &Content{
		ID:          rawContent.ID,
		Title:       rawContent.Title,
		Content:     utils.SanitizeHTML(rawContent.Body.View.Value),
		PublishDate: rawContent.Version.When,
		URL:         c.baseURL + rawContent.Links.Webui,
		Language:    rawContent.Language,
		Author:      rawContent.History.CreatedBy.DisplayName,
		Keywords:    strings.Join(contentKeywords, ", "),
		Description: rawContent.Description,
	}

	return content, nil
}

// GetAttachment retrieves a single attachment by ID
func (c *Client) GetAttachment(attachmentID string) (*Attachment, error) {
	url := c.baseURL + "/rest/api/content/" + attachmentID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	req = c.preparePageQuery(req, "", "", []string{"version", "history", "metadata.labels"})

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var rawAttachment RawAttachmentDetail

	if err := json.NewDecoder(resp.Body).Decode(&rawAttachment); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	attachment := &Attachment{
		ID:               rawAttachment.ID,
		Title:            rawAttachment.Title,
		Author:           rawAttachment.Version.By.DisplayName,
		LastModifiedDate: rawAttachment.Version.When,
		MediaType:        rawAttachment.Metadata.MediaType,
		Download:         c.baseURL + rawAttachment.Links.Download,
	}

	return attachment, nil
}

func (c *Client) GetSpaceContents(spaceKey string, processContent func(content Content) error) error {
	nextPageURL := c.baseURL + "/rest/api/space/" + spaceKey + "/content/page"

	for nextPageURL != "" {
		req, err := http.NewRequest("GET", nextPageURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %v", err)
		}

		if req.URL.Path == "/rest/api/space/"+spaceKey+"/content/page" {
			req = c.preparePageQuery(req, "1", "0", []string{"body.view", "version", "history", "children.attachment.version", "children.comment", "metadata.labels"})
		}

		resp, err := c.client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to send request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var result RawContent
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to unmarshal response: %v", err)
		}

		for _, content := range result.Results {
			var contentKeywords []string
			for _, label := range content.Metadata.Labels.Results {
				contentKeywords = append(contentKeywords, label.Name)
			}

			var attachment []Attachment
			for _, att := range content.Children.RawAttachment.Results {
				if !c.unsupportedTypes[att.Metadata.MediaType] {
					attachment = append(attachment, Attachment{
						ID:               att.ID,
						Title:            att.Title,
						Author:           att.Version.By.DisplayName,
						LastModifiedDate: att.Version.When,
						MediaType:        att.Metadata.MediaType,
						Download:         c.baseURL + att.Links.Download,
					})
				}
				if !c.allowedTypes[att.Metadata.MediaType] {
					fmt.Printf("Skipping attachment %s with media type %s\n", att.Title, att.Metadata.MediaType)
				}
			}

			contentItem := Content{
				ID:          content.ID,
				Title:       content.Title,
				Content:     utils.SanitizeHTML(content.Body.View.Value),
				PublishDate: content.Version.When, // RFC3339 format: "2017-02-27T12:16:24.000+01:00"
				URL:         c.baseURL + content.Links.Webui,
				Language:    content.Language,
				Author:      content.History.CreatedBy.DisplayName,
				Keywords:    strings.Join(contentKeywords, ", "),
				Description: content.Description,
				Attachment:  attachment,
			}
			if err := processContent(contentItem); err != nil {
				return fmt.Errorf("failed to process content: %v", err)
			}
		}

		if result.Links.Next != "" {
			nextPageURL = c.baseURL + result.Links.Next
		} else {
			nextPageURL = ""
		}
	}

	return nil
}
