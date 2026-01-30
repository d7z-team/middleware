package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.d7z.net/middleware/kv"
)

func TestTypedConfig(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	config := NewTypedConfig[int](memory, "config")

	// Store
	err = config.Store(t.Context(), "max_connections", 100)
	assert.NoError(t, err)

	// Load
	val, ok := config.Load(t.Context(), "max_connections")
	assert.True(t, ok)
	assert.Equal(t, 100, val)

	// Delete
	config.Delete(t.Context(), "max_connections")

	val, ok = config.Load(t.Context(), "max_connections")
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}
