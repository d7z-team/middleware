package tools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	load, found := cache.Load(t.Context(), "key")
	assert.True(t, found)
	assert.Equal(t, value, load)
}

func TestKVCache_LoadNonExistentKey(t *testing.T) {
	memory, err := kv.NewMemory("")
	assert.NoError(t, err)

	cache := NewCache[testStruct](memory, "cache", time.Second)

	// Test loading non-existent key
	_, found := cache.Load(t.Context(), "key1")
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
	_, found := cache.Load(t.Context(), "key")
	assert.True(t, found)

	// Wait for expiration
	time.Sleep(time.Second)

	// Key should not exist after expiration
	_, found = cache.Load(t.Context(), "key")
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
	load, found := cache.Load(t.Context(), "key2")
	assert.True(t, found)
	assert.Equal(t, value, load)

	// Delete the key
	cache.Delete(t.Context(), "key2")

	// Key should not exist after deletion
	_, found = cache.Load(t.Context(), "key2")
	assert.False(t, found)
}
