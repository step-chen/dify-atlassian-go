package utils

import (
	"net/http"
)

func AddBearerAuthHeader(apiKey string, req *http.Request) *http.Request {
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	return req
}
