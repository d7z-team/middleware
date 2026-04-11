package tools

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/kv"
)

type testStruct struct {
	K string `json:"k"`
	V string `json:"v"`
}

func TestKVCache_StoreAndLoad(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	cache := NewCache[testStruct](memory, "cache", time.Second)
	value := testStruct{
		K: "key",
		V: "val",
	}

	// Test storing and loading
	err = cache.Store(t.Context(), "key", value)
	assert.NoError(t, err)

	load, found, err := cache.Load(t.Context(), "key")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value, load)
}

func TestKVCache_LoadNonExistentKey(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	cache := NewCache[testStruct](memory, "cache", time.Second)

	// Test loading non-existent key
	_, found, err := cache.Load(t.Context(), "key1")
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestKVCache_Expiration(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	cache := NewCache[testStruct](memory, "cache", time.Second)
	value := testStruct{
		K: "key",
		V: "val",
	}

	// Test expiration
	err = cache.Store(t.Context(), "key", value)
	assert.NoError(t, err)

	// Key should exist before expiration
	_, found, err := cache.Load(t.Context(), "key")
	assert.NoError(t, err)
	assert.True(t, found)

	// Wait for expiration
	time.Sleep(time.Second)

	// Key should not exist after expiration
	_, found, err = cache.Load(t.Context(), "key")
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestKVCache_Delete(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	cache := NewCache[testStruct](memory, "cache", time.Second)
	value := testStruct{
		K: "key",
		V: "val",
	}

	// Test deletion
	err = cache.Store(t.Context(), "key2", value)
	assert.NoError(t, err)

	// Key should exist before deletion
	load, found, err := cache.Load(t.Context(), "key2")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value, load)

	// Delete the key
	err = cache.Delete(t.Context(), "key2")
	assert.NoError(t, err)

	// Key should not exist after deletion
	_, found, err = cache.Load(t.Context(), "key2")
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestKVCache_PointerType(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	// Use *testStruct as Data
	cache := NewCache[*testStruct](memory, "cache_ptr", time.Second)
	value := &testStruct{
		K: "key",
		V: "val",
	}

	// Test storing
	err = cache.Store(t.Context(), "key", value)
	assert.NoError(t, err)

	// Test loading
	load, found, err := cache.Load(t.Context(), "key")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, value, load)
}

func TestKVCache_LoadInvalidJSON(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	cache := NewCache[testStruct](memory, "cache", time.Second)

	require.NoError(t, memory.Child("cache").Put(t.Context(), "broken", "not-json", kv.TTLKeep))

	_, found, err := cache.Load(t.Context(), "broken")
	assert.Error(t, err)
	assert.False(t, found)
}

func TestKVCache_LoadContextError(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	cache := NewCache[testStruct](memory, "cache", time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, found, err := cache.Load(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, found)
}

func TestKVCache_DeleteContextError(t *testing.T) {
	memory, err := kv.NewMemory("")
	require.NoError(t, err)
	cache := NewCache[testStruct](memory, "cache", time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = cache.Delete(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
}
