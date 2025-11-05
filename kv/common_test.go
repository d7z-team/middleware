package kv

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/connects"
)

// 通用测试套件：所有 KV 实现必须通过此测试
func testKVConsistency(t *testing.T, kvClient KV) {
	t.Helper()
	ctx := context.Background()
	// 唯一前缀：避免测试污染（每次运行生成不同前缀）
	uniquePrefix := "kv_test_" + time.Now().Format("20060102150405.999") + "/"
	spliter := kvClient.Spliter()

	// 测试后清理数据 + 关闭客户端
	defer func() {
		// 关闭客户端
		err := kvClient.Close()
		assert.NoError(t, err)

		// 关闭后操作应返回错误
		key := uniquePrefix + "close_key"
		err = kvClient.Put(ctx, key, "close_val", TTLKeep)
		assert.ErrorIs(t, err, ErrClosed, "关闭后 Put 应返回 ErrClosed")

		_, err = kvClient.Get(ctx, key)
		assert.ErrorIs(t, err, ErrClosed, "关闭后 Get 应返回 ErrClosed")
	}()

	// ########################### 测试用例 ###########################
	// 1. 测试 Spliter（分隔符非空）
	t.Run("Spliter", func(t *testing.T) {
		assert.NotEmpty(t, spliter, "Spliter 不应为空")
		// 若所有实现强制使用统一分隔符（如 "/"），可添加：
		// assert.Equal(t, "/", spliter, "Spliter 必须为 '/'")
	})

	// 2. 测试 Put + Get（正常存入/获取、不存在键获取）
	t.Run("PutAndGet", func(t *testing.T) {
		key := uniquePrefix + "put_get_key"
		value := "test_value_123"

		// 存入键值对
		err := kvClient.Put(ctx, key, value, TTLKeep)
		assert.NoError(t, err, "Put 不应返回错误")

		// 获取存在的键
		getVal, err := kvClient.Get(ctx, key)
		assert.NoError(t, err, "Get 存在的键不应返回错误")
		assert.Equal(t, value, getVal, "获取的值应与存入的值一致")

		// 获取不存在的键
		nonExistentKey := uniquePrefix + "non_exist_key"
		getVal, err = kvClient.Get(ctx, nonExistentKey)
		assert.ErrorIs(t, err, ErrKeyNotFound, "获取不存在的键应返回 ErrKeyNotFound")
		assert.Empty(t, getVal, "不存在的键返回值应为空")
	})

	// 3. 测试 Delete（删除存在/不存在的键）
	t.Run("Delete", func(t *testing.T) {
		key := uniquePrefix + "delete_key"
		_ = kvClient.Put(ctx, key, "delete_val", TTLKeep)

		// 删除存在的键
		deleted, err := kvClient.Delete(ctx, key)
		assert.NoError(t, err)
		assert.True(t, deleted, "删除存在的键应返回 true")
		_, err = kvClient.Get(ctx, key)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		// 删除不存在的键
		deleted, err = kvClient.Delete(ctx, key)
		assert.NoError(t, err)
		assert.False(t, deleted, "删除不存在的键应返回 false")
	})

	// 4. 测试 PutIfNotExists（键不存在时存入、存在时不覆盖）
	t.Run("PutIfNotExists", func(t *testing.T) {
		key := uniquePrefix + "pin_key"
		val1 := "val1"
		val2 := "val2"

		// 键不存在：存入成功
		ok, err := kvClient.PutIfNotExists(ctx, key, val1, TTLKeep)
		assert.NoError(t, err)
		assert.True(t, ok)
		getVal, _ := kvClient.Get(ctx, key)
		assert.Equal(t, val1, getVal)

		// 键已存在：存入失败（值不覆盖）
		ok, err = kvClient.PutIfNotExists(ctx, key, val2, TTLKeep)
		assert.NoError(t, err)
		assert.False(t, ok)
		getVal, _ = kvClient.Get(ctx, key)
		assert.Equal(t, val1, getVal)
	})

	// 5. 测试 CompareAndSwap（原子CAS操作）
	t.Run("CompareAndSwap", func(t *testing.T) {
		key := uniquePrefix + "cas_key"
		oldVal := "old"
		newVal := "new"
		wrongOldVal := "wrong_old"

		_ = kvClient.Put(ctx, key, oldVal, TTLKeep)

		// 旧值匹配：CAS成功
		ok, err := kvClient.CompareAndSwap(ctx, key, oldVal, newVal)
		assert.NoError(t, err)
		assert.True(t, ok)
		getVal, _ := kvClient.Get(ctx, key)
		assert.Equal(t, newVal, getVal)

		// 旧值不匹配：CAS失败
		ok, err = kvClient.CompareAndSwap(ctx, key, wrongOldVal, "fail")
		assert.NoError(t, err)
		assert.False(t, ok)
		getVal, _ = kvClient.Get(ctx, key)
		assert.Equal(t, newVal, getVal)

		// 键不存在：CAS失败
		nonExistentKey := uniquePrefix + "cas_non_exist"
		ok, err = kvClient.CompareAndSwap(ctx, nonExistentKey, oldVal, newVal)
		assert.Error(t, err)
		assert.False(t, ok)
	})

	if kvClient, ok := kvClient.(PagedKV); ok {
		// 6. 测试 List（按前缀列出所有键值对）
		t.Run("List", func(t *testing.T) {
			prefix := uniquePrefix + "list/"
			// 存入 3 个带前缀的键 + 1 个不带前缀的键
			testKeys := map[string]string{
				prefix + "a":                   "a_val",
				prefix + "b":                   "b_val",
				prefix + "sub" + spliter + "c": "c_val",  // 子层级键
				uniquePrefix + "no_list":       "no_val", // 无前缀键
			}
			for k, v := range testKeys {
				_ = kvClient.Put(ctx, k, v, TTLKeep)
			}

			// 列出前缀下的键
			result, err := kvClient.List(ctx, prefix)
			assert.NoError(t, err)
			assert.Len(t, result, 3, "应返回 3 个带前缀的键")
			assert.Equal(t, "a_val", result[prefix+"a"])
			assert.Equal(t, "c_val", result[prefix+"sub"+spliter+"c"])
			assert.NotContains(t, result, uniquePrefix+"no_list", "不应包含无前缀键")
		})
	}
	// 7. 测试 ListPage（分页查询）
	if kvClient, ok := kvClient.(PagedKV); ok {
		t.Run("ListPage", func(t *testing.T) {
			prefix := uniquePrefix + "page/"
			pageSize := uint(2)
			// 存入 4 个有序键（假设实现按字典序排序）
			keys := []struct {
				key   string
				value string
			}{
				{prefix + "1", "v1"},
				{prefix + "2", "v2"},
				{prefix + "3", "v3"},
				{prefix + "4", "v4"},
			}
			for _, kv := range keys {
				_ = kvClient.Put(ctx, kv.key, kv.value, TTLKeep)
			}

			// 第 0 页（假设 pageIndex 从 0 开始）
			page0, err := kvClient.ListPage(ctx, prefix, 0, pageSize)
			assert.NoError(t, err)
			assert.Len(t, page0, 2)
			assert.Equal(t, "v1", page0[keys[0].key])
			assert.Equal(t, "v2", page0[keys[1].key])

			// 第 1 页
			page1, err := kvClient.ListPage(ctx, prefix, 1, pageSize)
			assert.NoError(t, err)
			assert.Len(t, page1, 2)
			assert.Equal(t, "v3", page1[keys[2].key])
			assert.Equal(t, "v4", page1[keys[3].key])

			// 第 2 页（超出范围）
			page2, err := kvClient.ListPage(ctx, prefix, 2, pageSize)
			assert.NoError(t, err)
			assert.Empty(t, page2, "超出范围的页应返回空")
		})
	}
	// 8. 测试 TTL（过期键自动删除、永久键存活）
	// 在测试TTL部分，增加等待时间和重试
	t.Run("TTL", func(t *testing.T) {
		// 带 TTL 的键（1秒过期）
		ttlKey := uniquePrefix + "ttl_key"
		err := kvClient.Put(ctx, ttlKey, "ttl_val", 1*time.Second)
		assert.NoError(t, err)

		// 立即获取（未过期）
		getVal, err := kvClient.Get(ctx, ttlKey)
		assert.NoError(t, err)
		assert.Equal(t, "ttl_val", getVal)

		// 等待过期（增加到2秒，确保etcd有足够时间清理）
		time.Sleep(2 * time.Second)

		// 使用重试机制，因为etcd的租约检查可能有延迟
		var lastErr error
		for i := 0; i < 5; i++ {
			_, lastErr = kvClient.Get(ctx, ttlKey)
			if errors.Is(lastErr, ErrKeyNotFound) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		assert.ErrorIs(t, lastErr, ErrKeyNotFound, "key should be expired and deleted")

		// 永久键（TTLKeep = -1）
		keepKey := uniquePrefix + "keep_key"
		err = kvClient.Put(ctx, keepKey, "keep_val", TTLKeep)
		assert.NoError(t, err)

		// 等待 0.5 秒后获取
		time.Sleep(500 * time.Millisecond)
		getVal, err = kvClient.Get(ctx, keepKey)
		assert.NoError(t, err)
		assert.Equal(t, "keep_val", getVal)
	})
}

// ########################### 具体实现测试 ###########################

// TestMemoryKV 测试内存实现（两种 Scheme：memory / storage）
func TestMemoryKV(t *testing.T) {
	// 测试 memory scheme（纯内存，不持久化）
	t.Run("memory-scheme", func(t *testing.T) {
		memKV, err := NewMemory("")
		require.NoError(t, err, "创建内存 KV 失败")
		testKVConsistency(t, memKV)
	})

	// 测试 storage scheme（持久化到本地文件）
	t.Run("storage-scheme", func(t *testing.T) {
		tempDir := t.TempDir() // 临时目录，测试后自动清理
		storageKV, err := NewMemory(filepath.Join(tempDir, "1.json"))
		require.NoError(t, err, "创建本地存储 KV 失败")
		testKVConsistency(t, storageKV)
	})
}

// TestRedisKV 测试 Redis 实现（需本地 Redis 服务）
func TestRedisKV(t *testing.T) {
	// 可选：跳过无 Redis 环境的测试

	// 1. 连接本地 Redis（默认配置）
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 无密码
		DB:       0,  // 默认数据库
	})
	ctx := context.Background()
	err := redisClient.Ping(ctx).Err()
	if err != nil {
		t.Skip("如需测试 Redis 实现，请确保本地 Redis 运行")
	}
	defer redisClient.Close()

	// 2. 创建 Redis KV 实例（需实现 NewRedis 函数）
	redisKV, err := NewRedis(redisClient, "kv_test/")
	require.NoError(t, err, "创建 Redis KV 失败")

	// 3. 运行一致性测试
	testKVConsistency(t, redisKV)
}

// TestRedisKV 测试 Redis 实现（需本地 Redis 服务）
func TestEtcdKV(t *testing.T) {
	parse, _ := url.Parse("etcd://127.0.0.1:2379")

	etcd, err := connects.NewEtcd(parse)
	if err != nil {
		t.Skip("如需测试 Redis 实现，请确保本地 Redis 运行")
	}
	defer etcd.Close()

	etcdKv := NewEtcd(etcd, "kv_test/")
	require.NoError(t, err, "创建 ETCD KV 失败")

	testKVConsistency(t, etcdKv)
}
