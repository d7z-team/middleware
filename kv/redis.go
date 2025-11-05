package kv

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/go-redis/redis/v8"
)

// 确保 KV 包中存在以下错误定义（已补充到 kv.go）
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrClosed      = errors.New("kv client closed")
	ErrCASFailed   = errors.New("compare and swap failed")
)

// RedisKV Redis 实现的 KV 接口
type RedisKV struct {
	client  *redis.Client
	prefix  string        // 键前缀（避免与其他数据冲突）
	spliter string        // 分隔符（与内存实现保持一致）
	closed  chan struct{} // 关闭状态标识
}

// NewRedis 创建 Redis KV 实例（供测试和业务使用）
func NewRedis(client *redis.Client, prefix string) (KV, error) {
	if client == nil {
		return nil, errors.New("redis client is nil")
	}
	return &RedisKV{
		client:  client,
		prefix:  prefix,
		spliter: "/",
		closed:  make(chan struct{}),
	}, nil
}

// Spliter 实现 KV 接口：返回键分隔符
func (r *RedisKV) Spliter() string {
	return r.spliter
}

// Put 实现 KV 接口：存入键值对（支持 TTL）
func (r *RedisKV) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	select {
	case <-r.closed:
		return ErrClosed
	default:
	}

	fullKey := r.prefix + key
	// TTLKeep（-1）对应 Redis 永久有效（expire=0）
	if ttl == TTLKeep {
		return r.client.Set(ctx, fullKey, value, 0).Err()
	}
	return r.client.Set(ctx, fullKey, value, ttl).Err()
}

// Get 实现 KV 接口：获取键值
func (r *RedisKV) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-r.closed:
		return "", ErrClosed
	default:
	}

	fullKey := r.prefix + key
	val, err := r.client.Get(ctx, fullKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrKeyNotFound
		}
		return "", err
	}
	return val, nil
}

// Delete 实现 KV 接口：删除键
func (r *RedisKV) Delete(ctx context.Context, key string) (bool, error) {
	select {
	case <-r.closed:
		return false, ErrClosed
	default:
	}

	fullKey := r.prefix + key
	delCount, err := r.client.Del(ctx, fullKey).Result()
	if err != nil {
		return false, err
	}
	return delCount > 0, nil
}

// PutIfNotExists 实现 KV 接口：键不存在时存入（原子操作）
func (r *RedisKV) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	select {
	case <-r.closed:
		return false, ErrClosed
	default:
	}

	fullKey := r.prefix + key
	var cmd *redis.BoolCmd

	// 原子操作：SETNX + EXPIRE（通过 Pipeline 保证原子性）
	pipe := r.client.Pipeline()
	defer pipe.Close()

	if ttl == TTLKeep {
		cmd = pipe.SetNX(ctx, fullKey, value, 0)
	} else {
		cmd = pipe.SetNX(ctx, fullKey, value, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	return cmd.Val(), nil
}

// CompareAndSwap 实现 KV 接口：CAS 原子操作（WATCH + MULTI + SET）
func (r *RedisKV) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	select {
	case <-r.closed:
		return false, ErrClosed
	default:
	}

	fullKey := r.prefix + key
	// 1. WATCH 监听键（防止期间被修改）
	if err := r.client.Watch(ctx, func(tx *redis.Tx) error {
		// 2. 检查当前值是否与旧值一致
		currentVal, err := tx.Get(ctx, fullKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return ErrKeyNotFound // 键不存在，返回特定错误
			}
			return err
		}
		if currentVal != oldValue {
			return ErrCASFailed
		}
		_, err = tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, fullKey, newValue, 0)
			return nil
		})
		return err
	}, fullKey); err != nil {
		if errors.Is(err, ErrCASFailed) {
			return false, nil
		}
		return false, err
	}

	// 5. 执行成功：返回 (true, nil)
	return true, nil
}

// List 实现 KV 接口：按前缀列出所有键值对（使用 SCAN 避免阻塞）
func (r *RedisKV) List(ctx context.Context, prefix string) (map[string]string, error) {
	select {
	case <-r.closed:
		return nil, ErrClosed
	default:
	}

	fullPrefix := r.prefix + prefix
	keys := make([]string, 0)
	cursor := uint64(0)

	// 1. SCAN 遍历所有符合前缀的键
	for {
		res, nextCursor, err := r.client.Scan(ctx, cursor, fullPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, res...)
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}

	// 2. MGET 批量获取值（减少网络往返）
	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// 3. 构造结果（去掉全局前缀，保留业务前缀）
	result := make(map[string]string, len(keys))
	for i, fullKey := range keys {
		// 去掉 RedisKV 的全局 prefix，返回业务侧的原始键
		bizKey := fullKey[len(r.prefix):]
		val, ok := values[i].(string)
		if ok && val != "" {
			result[bizKey] = val
		}
	}

	return result, nil
}

// ListPage 实现 KV 接口：按前缀分页查询（字典序排序）
func (r *RedisKV) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	// 1. 先获取所有符合前缀的键值对
	fullList, err := r.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// 2. 提取键并按字典序排序（保证分页一致性）
	keys := make([]string, 0, len(fullList))
	for k := range fullList {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 3. 计算分页边界
	start := pageIndex * uint64(pageSize)
	if start >= uint64(len(keys)) {
		return make(map[string]string), nil // 超出范围返回空
	}
	end := start + uint64(pageSize)
	if end > uint64(len(keys)) {
		end = uint64(len(keys))
	}

	// 4. 截取分页数据
	pageKeys := keys[start:end]
	pageResult := make(map[string]string, len(pageKeys))
	for _, k := range pageKeys {
		pageResult[k] = fullList[k]
	}

	return pageResult, nil
}

// Close 实现 KV 接口：关闭 Redis 连接
func (r *RedisKV) Close() error {
	select {
	case <-r.closed:
		return nil // 已关闭，避免重复关闭
	default:
		close(r.closed)
		return r.client.Close()
	}
}
