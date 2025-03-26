package dify

import "github.com/step-chen/dify-atlassian-go/internal/config"

// Indexing status constants
const (
	IndexingStatusCompleted = "completed" // Document indexing is complete
	IndexingStatusIndexing  = "indexing"  // Document is currently being indexed
)

// CreateDocumentRequest defines the request structure for creating a document
type CreateDocumentRequest struct {
	Name              string                 `json:"name"`               // Document name
	Text              string                 `json:"text"`               // Document content
	DocType           string                 `json:"doc_type"`           // Document type (e.g., "book", "web_page", "paper", "social_media_post", "wikipedia_entry", "personal_document", "business_document", "im_chat_log", "synced_from_notion", "synced_from_github", "others")
	DocMetadata       map[string]interface{} `json:"doc_metadata"`       // Document metadata
	IndexingTechnique string                 `json:"indexing_technique"` // Indexing technique (e.g. "high_quality", "economy")
	DocForm           string                 `json:"doc_form,omitempty"` // Document format (e.g. "text_model", "hierarchical_model", "qa_model")
	DocLanguage       string                 `json:"doc_language"`       // Document language (e.g. "English", "Chinese")
	ProcessRule       config.ProcessRule     `json:"process_rule"`       // Document processing rules
}

type CreateDocumentByFileRequest struct {
	OriginalDocumentID string                 `json:"original_document_id,omitempty"` // Source document ID (optional)
	IndexingTechnique  string                 `json:"indexing_technique"`             // Indexing technique to use
	DocForm            string                 `json:"doc_form,omitempty"`             // Document format (e.g. "text_model", "hierarchical_model", "qa_model")
	DocType            string                 `json:"doc_type"`                       // Document type (e.g., "book", "web_page", "paper", "social_media_post", "wikipedia_entry", "personal_document", "business_document", "im_chat_log", "synced_from_notion", "synced_from_github", "others")
	DocMetadata        map[string]interface{} `json:"doc_metadata,omitempty"`         // Document metadata
	DocLanguage        string                 `json:"doc_language"`                   // Document language (e.g. "English", "Chinese")
	ProcessRule        config.ProcessRule     `json:"process_rule"`                   // Document processing rules
}

func DefaultProcessRule(cfg *config.Config) config.ProcessRule {
	// Use config values if available, otherwise fall back to defaults
	var processRule config.ProcessRule
	if cfg != nil && cfg.Dify.RagSetting.ProcessRule != nil {
		processRule = *cfg.Dify.RagSetting.ProcessRule
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

// CreateDocumentResponse defines the response structure for document creation
type CreateDocumentResponse struct {
	Document struct {
		ID                   string                 `json:"id"`                      // Document ID
		Position             int                    `json:"position"`                // Document position
		DataSourceType       string                 `json:"data_source_type"`        // Data source type
		DataSourceInfo       map[string]interface{} `json:"data_source_info"`        // Data source information
		DataSourceDetailDict map[string]interface{} `json:"data_source_detail_dict"` // Detailed data source information
		DatasetProcessRuleID string                 `json:"dataset_process_rule_id"` // Dataset processing rule ID
		Name                 string                 `json:"name"`                    // Document name
		CreatedFrom          string                 `json:"created_from"`            // Source of document creation
		CreatedBy            string                 `json:"created_by"`              // Creator of the document
		CreatedAt            int64                  `json:"created_at"`              // Creation timestamp
		Tokens               int                    `json:"tokens"`                  // Number of tokens
		IndexingStatus       string                 `json:"indexing_status"`         // Current indexing status
		Error                interface{}            `json:"error"`                   // Any error that occurred
		Enabled              bool                   `json:"enabled"`                 // Whether the document is enabled
		DisabledAt           interface{}            `json:"disabled_at"`             // Timestamp when disabled
		DisabledBy           interface{}            `json:"disabled_by"`             // Who disabled the document
		Archived             bool                   `json:"archived"`                // Whether the document is archived
		DisplayStatus        string                 `json:"display_status"`          // Display status
		WordCount            int                    `json:"word_count"`              // Word count
		HitCount             int                    `json:"hit_count"`               // Number of hits
		DocForm              string                 `json:"doc_form"`                // Document format
	} `json:"document"`
	Batch string `json:"batch"` // Batch ID for the document creation
}

// IndexingStatusResponse defines the response structure for indexing status
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

// UpdateDocumentRequest defines the request structure for updating a document
type UpdateDocumentRequest struct {
	Name        string             `json:"name,omitempty"`
	Text        string             `json:"text,omitempty"`
	ProcessRule config.ProcessRule `json:"process_rule,omitempty"`
}

// DocumentListResponse defines the response structure for retrieving document list
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
