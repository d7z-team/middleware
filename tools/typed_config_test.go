package tools

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	val, ok, err := config.Load(t.Context(), "max_connections")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 100, val)

	// Delete
	err = config.Delete(t.Context(), "max_connections")
	assert.NoError(t, err)

	val, ok, err = config.Load(t.Context(), "max_connections")
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, 0, val)
}

func TestTypedConfig_MapType(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)

	config := NewTypedConfig[map[string]int](memory, "config")
	expected := map[string]int{"a": 1, "b": 2}

	require.NoError(t, config.Store(t.Context(), "thresholds", expected))

	val, ok, err := config.Load(t.Context(), "thresholds")
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, expected, val)
}

func TestTypedConfig_LoadInvalidJSON(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	config := NewTypedConfig[map[string]int](memory, "config")

	require.NoError(t, memory.Child("config").Put(t.Context(), "broken", "not-json", kv.TTLKeep))

	_, ok, err := config.Load(t.Context(), "broken")
	assert.Error(t, err)
	assert.False(t, ok)
}

func TestTypedConfig_LoadContextError(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	config := NewTypedConfig[int](memory, "config")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, ok, err := config.Load(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, ok)
}

func TestTypedConfig_DeleteContextError(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	config := NewTypedConfig[int](memory, "config")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = config.Delete(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
}
