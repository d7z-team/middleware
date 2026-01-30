package tools

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.d7z.net/middleware/cache"
)

func TestTTLCache(t *testing.T) {
	memCache, err := cache.NewMemoryCache(cache.MemoryCacheConfig{
		MaxCapacity: 100,
	})
	assert.NoError(t, err)

	ttl := time.Second
	ttlCache := NewTTLCache(memCache, ttl)

	key := "test_key"
	val := "test_val"
	metadata := map[string]string{"type": "test"}

	// Test Put
	err = ttlCache.Put(t.Context(), key, metadata, strings.NewReader(val))
	assert.NoError(t, err)

	// Test Get
	content, err := ttlCache.Get(t.Context(), key)
	assert.NoError(t, err)
	assert.Equal(t, metadata, content.Metadata)

	// Read content
	readVal, err := io.ReadAll(content)
	assert.NoError(t, err)
	assert.Equal(t, val, string(readVal))

	// Test Expiration (Wait for TTL)
	time.Sleep(ttl + 100*time.Millisecond)
	_, err = ttlCache.Get(t.Context(), key)
	assert.ErrorIs(t, err, cache.ErrCacheMiss)

	// Test Delete
	err = ttlCache.Put(t.Context(), key, metadata, strings.NewReader(val))
	assert.NoError(t, err)
	err = ttlCache.Delete(t.Context(), key)
	assert.NoError(t, err)
	_, err = ttlCache.Get(t.Context(), key)
	assert.ErrorIs(t, err, cache.ErrCacheMiss)
}
