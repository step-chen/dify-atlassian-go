package confluence

import (
	"net/http"
)

// Client represents a Confluence v1 API client
type Client struct {
	baseURL          string          // Base URL of the Confluence instance
	apiKey           string          // API key for authentication
	client           *http.Client    // HTTP client for making requests
	allowedTypes     map[string]bool // Allowed media types for attachments
	unsupportedTypes map[string]bool // Unsupported media types to skip
}

// Attachment represents an attachment
type AttachmentBasicInfo struct {
	LastModifiedDate string
	MediaType        string
}

// Attachment represents an attachment
type Attachment struct {
	ID               string `json:"id"`
	Title            string `json:"title"`
	Author           string `json:"author"`
	LastModifiedDate string `json:"last_modified_date"`
	MediaType        string `json:"mediaType"`
	Download         string `json:"download"`
}

type ContentOperation struct {
	Action           int8 // 0: create, 1: update, 2: delete, 3: no action
	Type             int8 // 0: page, 1: attachment
	LastModifiedDate string
	MediaType        string // Mime type
	DifyID           string
	DatasetID        string
}

type Content struct {
	ID          string       `json:"-"`
	Title       string       `json:"title"`
	URL         string       `json:"url"`
	Language    string       `json:"language"`
	PublishDate string       `json:"publish_date"`
	Author      string       `json:"author/publisher"`
	Keywords    string       `json:"topic/keywords"`
	Description string       `json:"description"`
	Content     string       `json:"-"`
	Attachment  []Attachment `json:"-"`
	DifyID      string       `json:"-"`
}

type Links struct {
	Next  string `json:"next"`
	Self  string `json:"self"`
	Webui string `json:"webui"`
}

type RawAttachmentBasicInfo struct {
	Results []struct {
		ID      string `json:"id"`
		Version struct {
			When string `json:"when"`
		} `json:"version"`
		Metadata struct {
			MediaType string `json:"mediaType"`
		} `json:"metadata"`
	} `json:"results"`
}

type RawAttachment struct {
	Results []struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Title   string `json:"title"`
		Version struct {
			By struct {
				DisplayName string `json:"displayName"`
			} `json:"by"`
			When string `json:"when"`
		} `json:"version"`
		Metadata struct {
			MediaType string `json:"mediaType"`
		} `json:"metadata"`
		Links struct {
			Download string `json:"download"`
		} `json:"_links"`
	} `json:"results"`
}

type RawContentOperation struct {
	Results []struct {
		ID      string `json:"id"`
		Version struct {
			When string `json:"when"`
		} `json:"version"`
		Children struct {
			RawAttachment `json:"attachment"`
		} `json:"children"`
	} `json:"results"`
	Links struct {
		Next string `json:"next"`
	} `json:"_links"`
}

type RawContent struct {
	Results []struct {
		ID    string `json:"id"`
		Title string `json:"title"`
		Body  struct {
			View struct {
				Value string `json:"value"`
			} `json:"view"`
		} `json:"body"`
		Version struct {
			When string `json:"when"`
		} `json:"version"`
		Links struct {
			Webui string `json:"webui"`
		} `json:"_links"`
		Language    string `json:"language"`
		Description string `json:"description"`
		Metadata    struct {
			Labels struct {
				Results []struct {
					Name string `json:"name"`
				} `json:"results"`
			} `json:"labels"`
		} `json:"metadata"`
		History struct {
			CreatedBy struct {
				DisplayName string `json:"displayName"`
			} `json:"createdBy"`
		} `json:"history"`
		Children struct {
			RawAttachment `json:"attachment"`
		} `json:"children"`
	} `json:"results"`
	Links Links `json:"_links"`
}

type RawContentDetail struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Body  struct {
		View struct {
			Value string `json:"value"`
		} `json:"view"`
	} `json:"body"`
	Version struct {
		When string `json:"when"`
	} `json:"version"`
	Links struct {
		Webui string `json:"webui"`
	} `json:"_links"`
	Language    string `json:"language"`
	Description string `json:"description"`
	History     struct {
		CreatedBy struct {
			DisplayName string `json:"displayName"`
		} `json:"createdBy"`
	} `json:"history"`
	Metadata struct {
		Labels struct {
			Results []struct {
				Name string `json:"name"`
			} `json:"results"`
		} `json:"labels"`
	} `json:"metadata"`
}

type RawAttachmentDetail struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Version struct {
		By struct {
			DisplayName string `json:"displayName"`
		} `json:"by"`
		When string `json:"when"`
	} `json:"version"`
	Metadata struct {
		MediaType string `json:"mediaType"`
	} `json:"metadata"`
	Links struct {
		Download string `json:"download"`
	} `json:"_links"`
}
