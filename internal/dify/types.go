package dify

import (
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/config"
)

// Indexing status constants
const (
	IndexingStatusCompleted = "completed" // Document indexing is complete
	IndexingStatusIndexing  = "indexing"  // Document is currently being indexed
)

// CreateDocumentRequest contains parameters for document creation
// Name: Document title
// Text: Document content
// DocType: Document category (e.g. "book", "web_page")
// DocMetadata: Additional document metadata
// IndexingTechnique: Indexing method (e.g. "high_quality")
// DocForm: Document structure format
// DocLanguage: Document language code
// ProcessRule: Document processing rules
type CreateDocumentRequest struct {
	Name              string             `json:"name"`               // Document name
	Text              string             `json:"text"`               // Document content
	IndexingTechnique string             `json:"indexing_technique"` // Indexing technique (e.g. "high_quality", "economy")
	DocForm           string             `json:"doc_form,omitempty"` // Document format (e.g. "text_model", "hierarchical_model", "qa_model")
	DocLanguage       string             `json:"doc_language"`       // Document language (e.g. "English", "Chinese")
	ProcessRule       config.ProcessRule `json:"process_rule"`       // Document processing rules
}

// CreateDocumentByFileRequest contains parameters for file-based document creation
// OriginalDocumentID: Source document ID (optional)
// IndexingTechnique: Indexing method to use
// DocForm: Document structure format
// DocType: Document category
// DocMetadata: Additional document metadata
// DocLanguage: Document language code
// ProcessRule: Document processing rules
type CreateDocumentByFileRequest struct {
	OriginalDocumentID string                 `json:"original_document_id,omitempty"` // Source document ID (optional)
	IndexingTechnique  string                 `json:"indexing_technique"`             // Indexing technique to use
	DocForm            string                 `json:"doc_form,omitempty"`             // Document format (e.g. "text_model", "hierarchical_model", "qa_model")
	DocType            string                 `json:"doc_type"`                       // Document type (e.g., "book", "web_page", "paper", "social_media_post", "wikipedia_entry", "personal_document", "business_document", "im_chat_log", "synced_from_notion", "synced_from_github", "others")
	DocMetadata        map[string]interface{} `json:"doc_metadata,omitempty"`         // Document metadata
	DocLanguage        string                 `json:"doc_language"`                   // Document language (e.g. "English", "Chinese")
	ProcessRule        config.ProcessRule     `json:"process_rule"`                   // Document processing rules
}

// DefaultProcessRule provides default document processing rules
// cfgProvider: Configuration provider to override defaults
// Returns configured ProcessRule with fallback values
func DefaultProcessRule(cfgProvider config.DifyCfgProvider) config.ProcessRule {
	// Use config values if available, otherwise fall back to defaults
	var processRule config.ProcessRule
	if cfgProvider != nil {
		difyCfg := cfgProvider.GetDifyConfig() // Get Dify config via interface
		if difyCfg.RagSetting.ProcessRule != nil {
			processRule = *difyCfg.RagSetting.ProcessRule
		}
	}
	if processRule.Mode == "" {
		processRule.Mode = "custom"
	}
	if processRule.Rules.PreProcessingRules == nil {
		processRule.Rules.PreProcessingRules = []config.PreprocessingRules{
			{
				ID:      "remove_extra_spaces",
				Enabled: true,
			},
			{
				ID:      "remove_urls_emails",
				Enabled: false,
			},
		}
	}
	if processRule.Rules.Segmentation.Separator == "" {
		processRule.Rules.Segmentation.Separator = "\n"
	}
	if processRule.Rules.Segmentation.MaxTokens == 0 {
		processRule.Rules.Segmentation.MaxTokens = 500
	}
	if processRule.Rules.ParentMode == "" {
		processRule.Rules.ParentMode = "full-doc"
	}
	if processRule.Rules.SubchunkSegmentation.Separator == "" {
		processRule.Rules.SubchunkSegmentation.Separator = "***"
	}
	if processRule.Rules.SubchunkSegmentation.MaxTokens == 0 {
		processRule.Rules.SubchunkSegmentation.MaxTokens = 500
	}
	if processRule.Rules.SubchunkSegmentation.ChunkOverlap == 0 {
		processRule.Rules.SubchunkSegmentation.ChunkOverlap = 150
	}

	return processRule
}

// CreateDocumentResponse contains document creation results
// Document: Created document details
// Batch: Processing batch ID
type CreateDocumentResponse struct {
	Document struct {
		ID             string      `json:"id"`              // Document ID
		Name           string      `json:"name"`            // Document name
		IndexingStatus string      `json:"indexing_status"` // Current indexing status
		Error          interface{} `json:"error"`           // Any error that occurred
	} `json:"document"`
	Batch string `json:"batch"` // Batch ID for the document creation
}

// IndexingStatusResponse contains document indexing progress
// Data: Array of indexing status records
type IndexingStatusResponse struct {
	Data []struct {
		ID                   string  `json:"id"`
		IndexingStatus       string  `json:"indexing_status"`
		ProcessingStartedAt  float64 `json:"processing_started_at"`
		ParsingCompletedAt   float64 `json:"parsing_completed_at"`
		CleaningCompletedAt  float64 `json:"cleaning_completed_at"`
		SplittingCompletedAt float64 `json:"splitting_completed_at"`
		CompletedAt          float64 `json:"completed_at"`
		PausedAt             float64 `json:"paused_at"`
		Error                string  `json:"error"`
		StoppedAt            float64 `json:"stopped_at"`
		CompletedSegments    int     `json:"completed_segments"`
		TotalSegments        int     `json:"total_segments"`
	} `json:"data"`
}

func (i *IndexingStatusResponse) LastStepAt() time.Time {
	if len(i.Data) == 0 {
		return time.Time{}
	}

	// Get the first record
	record := i.Data[0]

	// Convert all timestamps to time.Time
	timestamps := []time.Time{
		time.Unix(int64(record.ProcessingStartedAt), 0),
		time.Unix(int64(record.ParsingCompletedAt), 0),
		time.Unix(int64(record.CleaningCompletedAt), 0),
		time.Unix(int64(record.SplittingCompletedAt), 0),
		time.Unix(int64(record.CompletedAt), 0),
		time.Unix(int64(record.PausedAt), 0),
		time.Unix(int64(record.StoppedAt), 0),
	}

	// Find the latest timestamp
	latest := time.Time{}
	for _, t := range timestamps {
		if t.After(latest) {
			latest = t
		}
	}

	return latest
}

// UpdateDocumentRequest contains parameters for document updates
// Name: New document title (optional)
// Text: Updated document content (optional)
// ProcessRule: Updated processing rules (optional)
type UpdateDocumentRequest struct {
	Name        string             `json:"name,omitempty"`
	Text        string             `json:"text,omitempty"`
	ProcessRule config.ProcessRule `json:"process_rule,omitempty"`
}

// DocumentListResponse contains paginated document list
// Data: Array of document records
// Total: Total document count
// HasMore: Indicates if more documents are available
// Page: Current page number
type DocumentListResponse struct {
	Data []struct {
		ID          string `json:"id"`
		DocMetadata []struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"doc_metadata"`
	} `json:"data"`
	Total   int  `json:"total"`
	HasMore bool `json:"has_more"`
	Page    int  `json:"page"`
}

// DocumentMetadataRecord holds the metadata associated with a Dify document,
// linking it back to its source (Confluence or Bitbucket).
// It is used both for storing the metadata state and as parameters for updates.
type DocumentMetadataRecord struct {
	URL               string // Source URL (Confluence page/attachment or Bitbucket file)
	SourceType        string // "confluence", "file" or "git"
	Type              string // Content type ("page", "attachment", or "file")
	SpaceKey          string // Confluence space key (for Confluence documents)
	RepositorySlug    string // Bitbucket repository slug (for Bitbucket documents)
	FilePath          string // Bitbucket file path (for Bitbucket documents)
	ConfluenceIDs     string // Comma-separated list of associated Confluence content IDs (stored state)
	ConfluenceIDToAdd string `json:"-"` // Transient field: Confluence ID to add during an update operation. Ignored by JSON marshalling.
	When              string // Last modified timestamp (RFC3339 format)
	DifyID            string // The corresponding Dify document ID
	Xxh3              string // XXH3 hash of the content
}

// DocumentMetadata represents a single key-value pair for Dify's metadata API.
type DocumentMetadata struct {
	ID    string `json:"id"`
	Value string `json:"value"`
	Name  string `json:"name"`
}

type DocumentOperation struct {
	DocumentID   string             `json:"document_id"`
	MetadataList []DocumentMetadata `json:"metadata_list"`
}

type UpdateDocumentMetadataRequest struct {
	OperationData []DocumentOperation `json:"operation_data"`
}

// LocalFileMetadata holds processed metadata relevant for local file synchronization.
type LocalFileMetadata struct {
	DifyDocumentID string    // Dify's internal document ID
	DocID          string    // Our internal identifier (e.g., hash of relative path)
	OriginalPath   string    // The original absolute path of the file when indexed
	LastModified   time.Time // Last modification time stored in Dify metadata
	ContentHash    string    // XXH3 hash of the content stored in Dify metadata
}

// Segment represents a single chunk to be added.
type Segment struct {
	Content string `json:"content"`
}

// AddChunksRequest is the request body for adding segments/chunks to a document.
type AddChunksRequest struct {
	Segments []Segment `json:"segments"`
}
