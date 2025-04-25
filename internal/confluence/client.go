package confluence

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

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

func (c *Client) DownloadAttachment(url string, fileName string, mediaType string) (content string, err error) {
	return utils.PrepareAttachmentMarkdown(url, c.apiKey, fileName, mediaType)
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

func (c *Client) GetSpaceContentsList(spaceKey string) (contents map[string]batchpool.Operation, err error) {
	nextPageURL := c.baseURL + "/rest/api/space/" + spaceKey + "/content/page"

	contents = make(map[string]batchpool.Operation)
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
			contents[content.ID] = batchpool.Operation{
				Action:           0,
				LastModifiedDate: content.Version.When,
				Type:             0,
			}

			// Add attachments
			for _, att := range content.Children.RawAttachment.Results {
				if !c.unsupportedTypes[att.Metadata.MediaType] {
					contents[att.ID] = batchpool.Operation{
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

func (c *Client) GetContent(contentID string, onlyTitle bool) (*Content, error) {
	url := c.baseURL + "/rest/api/content/" + contentID
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	if onlyTitle {
		req = c.preparePageQuery(req, "", "", []string{"version", "history", "metadata.labels"})
	} else {
		req = c.preparePageQuery(req, "", "", []string{"body.view", "version", "history", "metadata.labels"})
	}

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
		Type:        "page",
		Title:       rawContent.Title,
		PublishDate: rawContent.Version.When,
		URL:         c.baseURL + rawContent.Links.Webui,
		Language:    rawContent.Language,
		Author:      rawContent.History.CreatedBy.DisplayName,
		Keywords:    strings.Join(contentKeywords, ","),
		MediaType:   "text/html",
	}

	if onlyTitle {
		content.Content = utils.FormatTitle(rawContent.Title)
		content.Xxh3 = fmt.Sprintf("%d", utils.XXH3Hash(content.Content))
	} else {
		md := ""
		if rawContent.Body.View.Value == "" {
			content.Content = ""
			return content, nil
		}
		if md, err = utils.ConvertHTMLToMarkdown(rawContent.Body.View.Value); err != nil {
			md = utils.SanitizeHTML(rawContent.Body.View.Value)
		}
		if md != "" && md != "{}" {
			content.Content = utils.EnsureTitleInContent(md, rawContent.Title, "# ", "\n\n")
			content.Xxh3 = fmt.Sprintf("%d", utils.XXH3Hash(content.Content))
		} else {
			content.Content = ""
		}
	}

	return content, nil
}

func (c *Client) GetAttachment(attachmentID string, onlyTitle bool) (*Content, error) {
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

	a := &Content{
		ID:          rawAttachment.ID,
		Type:        "attachment",
		Title:       rawAttachment.Title,
		Author:      rawAttachment.Version.By.DisplayName,
		PublishDate: rawAttachment.Version.When,
		MediaType:   rawAttachment.Metadata.MediaType,
		URL:         c.baseURL + rawAttachment.Links.Download,
	}

	if onlyTitle {
		a.Content = utils.FormatTitle(utils.RemoveFileExtension(a.Title))
		a.Xxh3 = fmt.Sprintf("%d", utils.XXH3Hash(a.Content))
	} else {
		md := ""
		if md, err = utils.PrepareAttachmentMarkdown(a.URL, c.apiKey, a.Title, a.MediaType); err != nil {
			return nil, fmt.Errorf("failed to convert %s, %s with response: %v", a.Title, a.MediaType, err)
		}
		if md != "" && md != "{}" {
			a.Content = utils.EnsureTitleInContent(md, utils.RemoveFileExtension(a.Title), "# ", "\n\n")
			a.Xxh3 = fmt.Sprintf("%d", utils.XXH3Hash(a.Content))
		}
	}

	return a, nil
}
