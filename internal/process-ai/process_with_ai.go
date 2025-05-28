package processai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	CFG "github.com/step-chen/dify-atlassian-go/internal/config"
)

// OpenAIRequest represents the request body for OpenAI's /v1/chat/completions endpoint.
type OpenAIRequest struct {
	Model       string          `json:"model"`
	Messages    []OpenAIMessage `json:"messages"`
	Stream      bool            `json:"stream,omitempty"` // Set to false for non-streaming response
	Temperature float64         `json:"temperature,omitempty"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
}

// OpenAIMessage represents a message in the OpenAI chat completions format.
type OpenAIMessage struct {
	Role    string `json:"role"`    // "system", "user", "assistant"
	Content string `json:"content"` // The message content
}

// OpenAIResponse represents the response body from OpenAI's /v1/chat/completions endpoint.
type OpenAIResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int           `json:"index"`
		Message      OpenAIMessage `json:"message"`
		LogProbs     interface{}   `json:"logprobs"` // Can be null
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	Error struct { // Added for API errors
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code"`
	} `json:"error,omitempty"`
}

// ProcessTextWithAIConfig processes a given text using the provided AI configuration,
// adhering to the OpenAI v1 chat completions API.
func ProcessTextWithAIConfig(aiConfig CFG.AIConfig, inputText string) (string, error) {
	// Parse the base URL from the AI config
	baseURL, err := url.Parse(aiConfig.URL)
	if err != nil {
		return "", fmt.Errorf("invalid AI config URL: %w", err)
	}

	// Resolve the chat completions path relative to the base URL
	// This correctly handles the scheme, host, and existing path segments
	fullURL := baseURL.ResolveReference(&url.URL{Path: "v1/chat/completions"})

	// Construct messages array for OpenAI chat completions
	messages := []OpenAIMessage{}

	// Add a system prompt if provided in AI config
	if aiConfig.Prompt != "" {
		messages = append(messages, OpenAIMessage{
			Role:    "system",
			Content: aiConfig.Prompt,
		})
	}

	// Add the user's input text
	messages = append(messages, OpenAIMessage{
		Role:    "user",
		Content: inputText,
	})

	// Prepare the request payload for OpenAI chat completions
	requestPayload := OpenAIRequest{
		Model:    aiConfig.ModelName,
		Messages: messages,
		Stream:   false, // We want a single, complete response
		// Temperature: 0.7,
		// MaxTokens:   150,
	}

	jsonPayload, err := json.Marshal(requestPayload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal OpenAI request payload: %w", err)
	}

	// Create an HTTP client
	client := &http.Client{
		Timeout: 30 * time.Minute, // Increased timeout for potentially longer AI responses
	}

	// Create a new POST request
	req, err := http.NewRequest("POST", fullURL.String(), bytes.NewBuffer(jsonPayload))
	if err != nil {
		return "", fmt.Errorf("failed to create new HTTP request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	if aiConfig.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+aiConfig.APIKey)
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request to AI API: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body from AI API: %w", err)
	}

	// Check for HTTP errors first
	if resp.StatusCode != http.StatusOK {
		var apiError struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &apiError); err == nil && apiError.Error.Message != "" {
			return "", fmt.Errorf("AI API returned non-OK status: %s, error: %s", resp.Status, apiError.Error.Message)
		}
		return "", fmt.Errorf("AI API returned non-OK status: %s, body: %s", resp.Status, string(body))
	}

	// Parse the response
	var openAIResponse OpenAIResponse
	if err := json.Unmarshal(body, &openAIResponse); err != nil {
		return "", fmt.Errorf("failed to unmarshal OpenAI response: %w", err)
	}

	// Check for API-specific errors within the response body (if any)
	if openAIResponse.Error.Message != "" {
		return "", fmt.Errorf("AI processing failed: %s (Type: %s, Code: %s)",
			openAIResponse.Error.Message, openAIResponse.Error.Type, openAIResponse.Error.Code)
	}

	// Extract the generated text from the first choice
	if len(openAIResponse.Choices) > 0 {
		content := openAIResponse.Choices[0].Message.Content
		// Remove <think> sections from content
		re := regexp.MustCompile(`(?s)<think>.*?</think>`)
		return strings.TrimSpace(re.ReplaceAllString(content, "")), nil
	}

	return "", fmt.Errorf("no content received from AI response")
}
