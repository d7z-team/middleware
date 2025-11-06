package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

// CacheFactory 用于创建缓存实例的函数类型
type CacheFactory func(t *testing.T) Cache

// TestCache_Common 通用缓存接口测试套件
func testCache_Common(t *testing.T, factory CacheFactory) {
	t.Run("PutGet", func(t *testing.T) { testPutGet(t, factory) })
	t.Run("PutInvalidTTL", func(t *testing.T) { testPutInvalidTTL(t, factory) })
	t.Run("GetMiss", func(t *testing.T) { testGetMiss(t, factory) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, factory) })
	t.Run("Update", func(t *testing.T) { testUpdate(t, factory) })
	t.Run("TTL", func(t *testing.T) { testTTL(t, factory) })
	t.Run("Concurrency", func(t *testing.T) { testConcurrency(t, factory) })
	t.Run("Close", func(t *testing.T) { testClose(t, factory) })
}

// testPutGet 测试基本的 Put 和 Get 功能
func testPutGet(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "test-key"
	value := []byte("test-value")

	// 测试正常存入和读取
	err := cache.Put(ctx, key, bytes.NewReader(value), TTLKeep)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	content, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer content.Close()

	// 验证内容
	data, err := io.ReadAll(content)
	if err != nil {
		t.Fatalf("Read content failed: %v", err)
	}

	if !bytes.Equal(data, value) {
		t.Errorf("Content mismatch: expected %q, got %q", value, data)
	}

	// 验证元数据
	if content.Length != len(value) {
		t.Errorf("Length mismatch: expected %d, got %d", len(value), content.Length)
	}

	if content.LastModified.IsZero() {
		t.Error("LastModified should not be zero")
	}
}

// testPutInvalidTTL 测试无效 TTL 的处理
func testPutInvalidTTL(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "test-key"
	value := []byte("test-value")

	// 测试负 TTL
	err := cache.Put(ctx, key, bytes.NewReader(value), -time.Second)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("Expected ErrInvalidTTL for negative TTL, got: %v", err)
	}

	// 测试零 TTL
	err = cache.Put(ctx, key, bytes.NewReader(value), 0)
	if !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("Expected ErrInvalidTTL for zero TTL, got: %v", err)
	}
}

// testGetMiss 测试缓存未命中的情况
func testGetMiss(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "non-existent-key"

	_, err := cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("Expected ErrCacheMiss for non-existent key, got: %v", err)
	}
}

// testDelete 测试删除功能
func testDelete(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "delete-key"
	value := []byte("delete-value")

	// 先存入
	err := cache.Put(ctx, key, bytes.NewReader(value), TTLKeep)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 验证存在
	_, err = cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get before delete failed: %v", err)
	}

	// 删除
	err = cache.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 验证已删除
	_, err = cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("Expected ErrCacheMiss after delete, got: %v", err)
	}

	// 删除不存在的 key 应该不报错
	err = cache.Delete(ctx, "non-existent-key")
	if err != nil {
		t.Errorf("Delete non-existent key should not error, got: %v", err)
	}
}

// testUpdate 测试更新已存在的 key
func testUpdate(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "update-key"
	value1 := []byte("value1")
	value2 := []byte("value2-updated")

	// 第一次存入
	err := cache.Put(ctx, key, bytes.NewReader(value1), TTLKeep)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// 更新
	err = cache.Put(ctx, key, bytes.NewReader(value2), TTLKeep)
	if err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	// 验证更新后的值
	content, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	defer content.Close()

	data, err := io.ReadAll(content)
	if err != nil {
		t.Fatalf("Read content failed: %v", err)
	}

	if !bytes.Equal(data, value2) {
		t.Errorf("Content after update mismatch: expected %q, got %q", value2, data)
	}
}

// testTTL 测试 TTL 过期功能
func testTTL(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "ttl-key"
	value := []byte("ttl-value")
	ttl := 1 * time.Second

	// 存入带 TTL 的缓存
	err := cache.Put(ctx, key, bytes.NewReader(value), ttl)
	if err != nil {
		t.Fatalf("Put with TTL failed: %v", err)
	}

	// 立即获取应该存在
	_, err = cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get immediately after Put failed: %v", err)
	}

	// 等待 TTL 过期
	time.Sleep(ttl + 2*time.Second)

	// 过期后应该返回 CacheMiss
	_, err = cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("Expected ErrCacheMiss after TTL expiration, got: %v", err)
	}
}

// testConcurrency 测试并发安全性
func testConcurrency(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	const (
		goroutines = 10
		operations = 50
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j%5) // 限制 key 数量增加冲突概率
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))

				// 随机执行操作
				switch j % 3 {
				case 0: // Put
					err := cache.Put(ctx, key, bytes.NewReader(value), TTLKeep)
					if err != nil {
						t.Logf("Concurrent Put error: %v", err)
					}
				case 1: // Get
					content, err := cache.Get(ctx, key)
					if err != nil && !errors.Is(err, ErrCacheMiss) {
						t.Logf("Concurrent Get error: %v", err)
					}
					if content != nil {
						io.Copy(io.Discard, content) // 读取数据
						content.Close()
					}
				case 2: // Delete
					err := cache.Delete(ctx, key)
					if err != nil {
						t.Logf("Concurrent Delete error: %v", err)
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证缓存仍然可用
	key := "final-check"
	value := []byte("final-value")
	err := cache.Put(ctx, key, bytes.NewReader(value), TTLKeep)
	if err != nil {
		t.Errorf("Put after concurrency test failed: %v", err)
	}

	content, err := cache.Get(ctx, key)
	if err != nil {
		t.Errorf("Get after concurrency test failed: %v", err)
	} else {
		content.Close()
	}
}

// testClose 测试关闭功能
func testClose(t *testing.T, factory CacheFactory) {
	cache := factory(t)

	// 测试幂等性
	err := cache.Close()
	if err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	err = cache.Close()
	if err != nil {
		t.Errorf("Second Close (idempotent) failed: %v", err)
	}

	// 关闭后尝试操作（具体行为取决于实现，这里只测试不会 panic）
	ctx := context.Background()
	_ = cache.Put(ctx, "closed-key", bytes.NewReader([]byte("value")), TTLKeep)
	_, _ = cache.Get(ctx, "closed-key")
	_ = cache.Delete(ctx, "closed-key")
}

// TestCache_ErrorReader 测试读取错误的情况
func testCacheErrorReader(t *testing.T, factory CacheFactory) {
	cache := factory(t)
	defer cache.Close()

	ctx := context.Background()
	key := "error-key"

	// 创建会返回错误的 Reader
	errorReader := &errorReader{err: errors.New("mock read error")}

	err := cache.Put(ctx, key, errorReader, TTLKeep)
	if err == nil {
		t.Error("Expected error from faulty reader, but got none")
	}

	// 验证错误的 key 没有被存入
	_, err = cache.Get(ctx, key)
	if !errors.Is(err, ErrCacheMiss) {
		t.Errorf("Expected ErrCacheMiss after Put error, got: %v", err)
	}
}

func TestMemory(t *testing.T) {
	t.Run("Common", func(t *testing.T) {
		testCache_Common(t, func(t *testing.T) Cache {
			cache, err := NewMemoryCache(
				MemoryCacheConfig{
					100,
					time.Minute,
				},
			)
			if err != nil {
				t.Fatalf("Failed to create cache: %v", err)
			}
			return cache
		})
	})
	t.Run("ErrorReader", func(t *testing.T) {
		testCacheErrorReader(t, func(t *testing.T) Cache {
			cache, err := NewMemoryCache(
				MemoryCacheConfig{
					MaxCapacity: 10,
				},
			)
			if err != nil {
				t.Fatalf("Failed to create cache: %v", err)
			}
			return cache
		})
	})
}

func TestRedis(t *testing.T) {
	t.Run("Common", func(t *testing.T) {
		testCache_Common(t, func(t *testing.T) Cache {
			parse, _ := url.Parse("redis://127.0.0.1:6379")
			redis, err := connects.NewRedis(parse)
			if err != nil {
				t.Skip("Failed to connect to redis")
			}
			return NewRedisCache(redis, "common")
		})
	})
	t.Run("ErrorReader", func(t *testing.T) {
		testCacheErrorReader(t, func(t *testing.T) Cache {
			parse, _ := url.Parse("redis://127.0.0.1:6379")
			redis, err := connects.NewRedis(parse)
			if err != nil {
				t.Skip("Failed to connect to redis")
			}
			return NewRedisCache(redis, "reader")
		})
	})
}
