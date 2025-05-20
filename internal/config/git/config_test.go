package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	// Test loading config
	cfg, err := LoadConfig("../../../cmd/dify-git/config.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
}
