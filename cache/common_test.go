package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.d7z.net/middleware/connects"
)

// --- Test Helpers & Utilities ---

// CacheFactory 用于创建缓存实例的函数类型
type CacheFactory func(t *testing.T) CloserCache

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

type mockReadSeekCloser struct {
	*bytes.Reader
}

func (m *mockReadSeekCloser) Close() error { return nil }

// --- Common Test Suite ---

// testCacheCommon 通用缓存接口测试套件，涵盖了缓存实现应遵循的核心行为
func testCacheCommon(t *testing.T, factory CacheFactory) {
	t.Run("PutGet", func(t *testing.T) { testPutGet(t, factory) })
	t.Run("Update", func(t *testing.T) { testUpdate(t, factory) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, factory) })
	t.Run("GetMiss", func(t *testing.T) { testGetMiss(t, factory) })
	t.Run("TTL", func(t *testing.T) { testTTL(t, factory) })
	t.Run("PutInvalidTTL", func(t *testing.T) { testPutInvalidTTL(t, factory) })
	t.Run("Concurrency", func(t *testing.T) { testConcurrency(t, factory) })
	t.Run("Boundary", func(t *testing.T) { testBoundary(t, factory) })
	t.Run("ErrorReader", func(t *testing.T) { testCacheErrorReader(t, factory) })
}

func testPutGet(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key, value := "test-key", []byte("test-value")
	metadata := map[string]string{"version": "1.0"}

	assert.NoError(t, cache.Put(ctx, key, metadata, bytes.NewReader(value), TTLKeep))

	content, err := cache.Get(ctx, key)
	assert.NoError(t, err)
	defer content.Close()

	data, err := io.ReadAll(content)
	assert.NoError(t, err)
	assert.Equal(t, value, data)
	assert.Equal(t, metadata, content.Metadata)
}

func testUpdate(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "update-key"
	v1, v2 := []byte("v1"), []byte("v2")

	assert.NoError(t, cache.Put(ctx, key, nil, bytes.NewReader(v1), TTLKeep))
	assert.NoError(t, cache.Put(ctx, key, nil, bytes.NewReader(v2), TTLKeep))

	content, err := cache.Get(ctx, key)
	assert.NoError(t, err)
	defer content.Close()

	data, _ := io.ReadAll(content)
	assert.Equal(t, v2, data)
}

func testDelete(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "del-key"

	assert.NoError(t, cache.Put(ctx, key, nil, bytes.NewReader([]byte("data")), TTLKeep))
	assert.NoError(t, cache.Delete(ctx, key))

	_, err := cache.Get(ctx, key)
	assert.ErrorIs(t, err, ErrCacheMiss)

	// Idempotent delete
	assert.NoError(t, cache.Delete(ctx, "no-key"))
}

func testGetMiss(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	_, err := cache.Get(context.Background(), "missing")
	assert.ErrorIs(t, err, ErrCacheMiss)
}

func testTTL(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "ttl-key"
	ttl := 500 * time.Millisecond

	assert.NoError(t, cache.Put(ctx, key, nil, bytes.NewReader([]byte("data")), ttl))
	_, err := cache.Get(ctx, key)
	assert.NoError(t, err)

	time.Sleep(ttl + 100*time.Millisecond)
	_, err = cache.Get(ctx, key)
	assert.ErrorIs(t, err, ErrCacheMiss)
}

func testPutInvalidTTL(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	err := cache.Put(context.Background(), "key", nil, bytes.NewReader([]byte("d")), 0)
	assert.ErrorIs(t, err, ErrInvalidTTL)
}

func testConcurrency(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	const workers, ops = 5, 20
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("conf-%d", j%3)
				_ = cache.Put(ctx, key, nil, bytes.NewReader([]byte("v")), TTLKeep)
				_, _ = cache.Get(ctx, key)
			}
		}(i)
	}
	wg.Wait()
}

func testBoundary(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()
	ctx := context.Background()

	// Empty content
	assert.NoError(t, cache.Put(ctx, "empty", nil, bytes.NewReader([]byte{}), TTLKeep))
	c, _ := cache.Get(ctx, "empty")
	d, _ := io.ReadAll(c)
	assert.Empty(t, d)

	// Large content (512KB)
	large := make([]byte, 512*1024)
	assert.NoError(t, cache.Put(ctx, "large", nil, bytes.NewReader(large), TTLKeep))
	c, _ = cache.Get(ctx, "large")
	d, _ = io.ReadAll(c)
	assert.Len(t, d, 512*1024)

	// Special metadata
	meta := map[string]string{"key": "val with spaces", "unicode": "测试"}
	assert.NoError(t, cache.Put(ctx, "meta", meta, bytes.NewReader([]byte("d")), TTLKeep))
	c, _ = cache.Get(ctx, "meta")
	assert.Equal(t, meta, c.Metadata)
}

func testCacheErrorReader(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	reader := &errorReader{err: errors.New("read fail")}
	err := cache.Put(context.Background(), "err-key", nil, reader, TTLKeep)
	assert.Error(t, err)

	_, err = cache.Get(context.Background(), "err-key")
	assert.ErrorIs(t, err, ErrCacheMiss)
}

// --- Specific Implementation Tests ---

func TestCommonFunctions(t *testing.T) {
	t.Run("NewCacheFromURL", func(t *testing.T) {
		// 1. Memory with parameters
		c1, err := NewCacheFromURL("memory://?max_capacity=100&cleanup_interval=1m")
		assert.NoError(t, err)
		assert.NotNil(t, c1)
		_ = c1.Close()

		// 2. Memory with defaults
		c2, err := NewCacheFromURL("mem://")
		assert.NoError(t, err)
		assert.NotNil(t, c2)
		_ = c2.Close()

		// 3. Redis (basic parsing)
		c3, err := NewCacheFromURL("redis://localhost:6379/0?prefix=test:")
		if err == nil {
			assert.NotNil(t, c3)
			_ = c3.Close()
		}

		// 4. Error cases
		testCases := []struct {
			name string
			url  string
		}{
			{"InvalidURL", ":%"},
			{"UnsupportedScheme", "unknown://"},
			{"InvalidCapacity", "memory://?max_capacity=-1"},
			{"ZeroCapacity", "memory://?max_capacity=0"},
			{"NonIntCapacity", "memory://?max_capacity=abc"},
			{"InvalidInterval", "memory://?cleanup_interval=invalid"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := NewCacheFromURL(tc.url)
				assert.Error(t, err)
			})
		}
	})

	t.Run("ReadToString", func(t *testing.T) {
		c := &Content{
			ReadSeekCloser: &mockReadSeekCloser{Reader: bytes.NewReader([]byte("data"))},
		}
		s, err := c.ReadToString()
		assert.NoError(t, err)
		assert.Equal(t, "data", s)
	})
}

func TestMemoryImplementation(t *testing.T) {
	t.Run("Config", func(t *testing.T) {
		_, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 0})
		assert.ErrorIs(t, err, ErrInvalidCapacity)
	})

	t.Run("LRU", func(t *testing.T) {
		cache, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 1})
		defer cache.Close()
		ctx := context.Background()
		_ = cache.Put(ctx, "k1", nil, strings.NewReader("d1"), TTLKeep)
		_ = cache.Put(ctx, "k2", nil, strings.NewReader("d2"), TTLKeep)
		_, err := cache.Get(ctx, "k1")
		assert.ErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("TestSuite", func(t *testing.T) {
		testCacheCommon(t, func(t *testing.T) CloserCache {
			c, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 100})
			return c
		})
	})

	t.Run("ChildTestSuite", func(t *testing.T) {
		testCacheCommon(t, func(t *testing.T) CloserCache {
			root, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 100})
			return closerCache{Cache: root.Child("a/b"), closer: root.Close}
		})
	})
}

func TestRedisImplementation(t *testing.T) {
	factory := func(t *testing.T) CloserCache {
		u, _ := url.Parse("redis://127.0.0.1:6379")
		client, err := connects.NewRedis(u)
		if err != nil {
			t.Skip("Redis not available")
		}
		return closerCache{Cache: NewRedisCache(client, "test"), closer: client.Close}
	}

	t.Run("TestSuite", func(t *testing.T) {
		testCacheCommon(t, factory)
	})

	t.Run("ChildTestSuite", func(t *testing.T) {
		testCacheCommon(t, func(t *testing.T) CloserCache {
			c := factory(t)
			return closerCache{Cache: c.Child("child"), closer: c.Close}
		})
	})
}
