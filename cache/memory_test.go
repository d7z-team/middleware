package cache

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryCacheChildNormalization(t *testing.T) {
	cache, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 16})
	require.NoError(t, err)
	defer cache.Close()

	require.Same(t, cache, cache.Child("", "/", "///"))

	child, ok := cache.Child("/a/", "b//").(*MemoryCache)
	require.True(t, ok)
	require.Equal(t, "/a/b/", child.prefix)

	require.NoError(t, child.Put(context.Background(), "key", nil, strings.NewReader("value"), TTLKeep))

	content, err := cache.Get(context.Background(), "a/b/key")
	require.NoError(t, err)
	defer content.Close()

	data, err := content.ReadToString()
	require.NoError(t, err)
	require.Equal(t, "value", data)
}

func TestMemoryCacheClosedAndCanceledBranches(t *testing.T) {
	cache, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 16})
	require.NoError(t, err)

	require.NoError(t, cache.Put(context.Background(), "key", nil, strings.NewReader("value"), TTLKeep))

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = cache.Get(canceledCtx, "key")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, cache.Delete(canceledCtx, "key"), context.Canceled)

	require.NoError(t, cache.Close())
	require.ErrorIs(t, cache.Put(context.Background(), "key", nil, strings.NewReader("value"), TTLKeep), ErrCacheClosed)
	_, err = cache.Get(context.Background(), "key")
	require.ErrorIs(t, err, ErrCacheClosed)
	require.ErrorIs(t, cache.Delete(context.Background(), "key"), ErrCacheClosed)
}
