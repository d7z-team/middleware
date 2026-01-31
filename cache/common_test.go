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
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/connects"
)

// testCacheConsistency 是一个通用的缓存接口测试套件，确保不同实现的行为一致。
func testCacheConsistency(t *testing.T, cache Cache) {
	t.Helper()
	ctx := context.Background()
	uniquePrefix := "cache_test_" + time.Now().Format("20060102150405.000") + "_"

	// 1. 基础读写删除操作
	t.Run("Basic_Operations", func(t *testing.T) {
		key := uniquePrefix + "basic"
		value := []byte("hello cache")
		metadata := map[string]string{"content-type": "text/plain"}

		// Put
		err := cache.Put(ctx, key, metadata, bytes.NewReader(value), TTLKeep)
		assert.NoError(t, err)

		// Get
		content, err := cache.Get(ctx, key)
		assert.NoError(t, err)
		defer content.Close()

		data, err := io.ReadAll(content)
		assert.NoError(t, err)
		assert.Equal(t, value, data)
		assert.Equal(t, metadata, content.Metadata)

		// Delete
		err = cache.Delete(ctx, key)
		assert.NoError(t, err)

		// Get after Delete
		_, err = cache.Get(ctx, key)
		assert.ErrorIs(t, err, ErrCacheMiss)
	})

	// 2. 更新操作 (Overwrite)
	t.Run("Update_Overwrite", func(t *testing.T) {
		key := uniquePrefix + "update"
		v1, v2 := []byte("value1"), []byte("value2")

		_ = cache.Put(ctx, key, nil, bytes.NewReader(v1), TTLKeep)
		_ = cache.Put(ctx, key, nil, bytes.NewReader(v2), TTLKeep)

		content, err := cache.Get(ctx, key)
		assert.NoError(t, err)
		defer content.Close()

		data, _ := io.ReadAll(content)
		assert.Equal(t, v2, data)
	})

	// 3. TTL 过期处理
	t.Run("TTL_Expiration", func(t *testing.T) {
		key := uniquePrefix + "ttl"
		ttl := 500 * time.Millisecond

		err := cache.Put(ctx, key, nil, bytes.NewReader([]byte("data")), ttl)
		assert.NoError(t, err)

		// 立即获取应成功
		_, err = cache.Get(ctx, key)
		assert.NoError(t, err)

		// 等待过期
		time.Sleep(ttl + 200*time.Millisecond)
		_, err = cache.Get(ctx, key)
		assert.ErrorIs(t, err, ErrCacheMiss)
	})

	// 4. 层级隔离 (Child)
	t.Run("Hierarchy_Isolation", func(t *testing.T) {
		parentKey := uniquePrefix + "parent_key"
		childKey := "child_key"

		child := cache.Child(uniquePrefix + "subdir")

		// 写入父级
		_ = cache.Put(ctx, parentKey, nil, bytes.NewReader([]byte("p")), TTLKeep)
		// 写入子级
		_ = child.Put(ctx, childKey, nil, bytes.NewReader([]byte("c")), TTLKeep)

		// 子级不应直接看到父级的 key (除非路径重合，但在隔离设计下不应)
		_, err := child.Get(ctx, parentKey)
		assert.ErrorIs(t, err, ErrCacheMiss)

		// 通过父级应能看到完整路径
		fullPath := uniquePrefix + "subdir/" + childKey
		content, err := cache.Get(ctx, fullPath)
		assert.NoError(t, err)
		content.Close()
	})

	// 5. 并发安全
	t.Run("Concurrency_Safety", func(t *testing.T) {
		const workers, ops = 10, 50
		var wg sync.WaitGroup
		wg.Add(workers)

		for i := 0; i < workers; i++ {
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < ops; j++ {
					key := fmt.Sprintf("%sconc_%d", uniquePrefix, j%5)
					_ = cache.Put(ctx, key, nil, bytes.NewReader([]byte("v")), TTLKeep)
					c, err := cache.Get(ctx, key)
					if err == nil {
						c.Close()
					}
				}
			}(i)
		}
		wg.Wait()
	})

	// 6. Context 取消支持
	t.Run("Context_Cancellation", func(t *testing.T) {
		cCtx, cancel := context.WithCancel(ctx)
		cancel() // 立即取消

		key := uniquePrefix + "canceled"
		err := cache.Put(cCtx, key, nil, bytes.NewReader([]byte("d")), TTLKeep)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), context.Canceled.Error())
	})

	// 7. 边界条件
	t.Run("Boundary_Conditions", func(t *testing.T) {
		// 空内容
		assert.NoError(t, cache.Put(ctx, uniquePrefix+"empty", nil, bytes.NewReader([]byte{}), TTLKeep))
		c, err := cache.Get(ctx, uniquePrefix+"empty")
		assert.NoError(t, err)
		d, _ := io.ReadAll(c)
		assert.Empty(t, d)
		c.Close()

		// 无效 TTL
		err = cache.Put(ctx, uniquePrefix+"invalid_ttl", nil, bytes.NewReader([]byte("d")), 0)
		assert.ErrorIs(t, err, ErrInvalidTTL)

		// 特殊字符 Key
		specialKey := uniquePrefix + "key with spaces and / and 中文"
		assert.NoError(t, cache.Put(ctx, specialKey, nil, bytes.NewReader([]byte("v")), TTLKeep))
		c, err = cache.Get(ctx, specialKey)
		assert.NoError(t, err)
		c.Close()

		// 极大元数据
		largeMeta := make(map[string]string)
		for i := 0; i < 100; i++ {
			largeMeta[fmt.Sprintf("key_%d", i)] = strings.Repeat("v", 100)
		}
		assert.NoError(t, cache.Put(ctx, uniquePrefix+"large_meta", largeMeta, bytes.NewReader([]byte("d")), TTLKeep))
	})
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
		content := "some data"
		c := &Content{
			ReadSeekCloser: &mockReadSeekCloser{Reader: strings.NewReader(content)},
		}
		s, err := c.ReadToString()
		assert.NoError(t, err)
		assert.Equal(t, content, s)

		// Test error on read (mock)
		errContent := &Content{
			ReadSeekCloser: &mockErrorReadCloser{err: errors.New("read error")},
		}
		_, err = errContent.ReadToString()
		assert.Error(t, err)
	})
}

type mockReadSeekCloser struct {
	*strings.Reader
}

func (m *mockReadSeekCloser) Close() error { return nil }
func (m *mockReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return m.Reader.Seek(offset, whence)
}

type mockErrorReadCloser struct {
	err error
}

func (m *mockErrorReadCloser) Read(p []byte) (n int, err error) { return 0, m.err }
func (m *mockErrorReadCloser) Close() error                     { return nil }
func (m *mockErrorReadCloser) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("not supported")
}

func TestMemoryImplementation(t *testing.T) {
	t.Run("Config", func(t *testing.T) {
		_, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 0})
		assert.ErrorIs(t, err, ErrInvalidCapacity)
	})

	t.Run("LRU_Eviction", func(t *testing.T) {
		cache, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 1})
		defer cache.Close()
		ctx := context.Background()
		_ = cache.Put(ctx, "k1", nil, strings.NewReader("d1"), TTLKeep)
		_ = cache.Put(ctx, "k2", nil, strings.NewReader("d2"), TTLKeep)
		_, err := cache.Get(ctx, "k1")
		assert.ErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("CleanupExpired", func(t *testing.T) {
		cache, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 10, CleanupInt: time.Hour})
		defer cache.Close()
		ctx := context.Background()

		_ = cache.Put(ctx, "expired", nil, strings.NewReader("data"), 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		// 手动调用内部方法（由于测试在同一个包下，可以访问私有方法）
		cache.cleanupExpired()

		_, err := cache.Get(ctx, "expired")
		assert.ErrorIs(t, err, ErrCacheMiss)
	})

	t.Run("ConsistencySuite", func(t *testing.T) {
		c, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 100})
		require.NoError(t, err)
		defer c.Close()
		testCacheConsistency(t, c)
	})

	t.Run("ChildConsistencySuite", func(t *testing.T) {
		root, err := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 100})
		require.NoError(t, err)
		defer root.Close()
		testCacheConsistency(t, root.Child("a/b"))
	})
}

func TestRedisImplementation(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	client, err := connects.NewRedis(u)
	if err != nil {
		t.Skip("Redis not available")
	}
	defer client.Close()

	t.Run("SpecificMethods", func(t *testing.T) {
		c := NewRedisCache(client, "test_specific")
		ctx := context.Background()
		key := "exists_key"

		_ = c.Put(ctx, key, nil, strings.NewReader("v"), 10*time.Second)

		exists, err := c.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)

		ttl, err := c.TTL(ctx, key)
		assert.NoError(t, err)
		assert.Positive(t, ttl)

		// cleanupCorruptedData test (internal)
		c.cleanupCorruptedData(ctx, c.dataKey(key), c.metaKey(key))
		exists, _ = c.Exists(ctx, key)
		assert.False(t, exists)
	})

	t.Run("ConsistencySuite", func(t *testing.T) {
		c := NewRedisCache(client, "test_consistency")
		testCacheConsistency(t, c)
	})

	t.Run("ChildConsistencySuite", func(t *testing.T) {
		root := NewRedisCache(client, "test_root")
		testCacheConsistency(t, root.Child("child"))
	})
}
