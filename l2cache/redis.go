package l2cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-redis/redis/v8"
)

// redisCacheMeta Redis缓存元数据（存储长度和最后修改时间）
type redisCacheMeta struct {
	Length       int       `json:"length"`
	LastModified time.Time `json:"last_modified"`
}

// RedisCache Redis缓存实现
type RedisCache struct {
	client *redis.Client // Redis客户端实例
	prefix string        // 缓存键前缀（避免键冲突）
}

// NewRedisCache 创建Redis缓存实例
// client: 已初始化的Redis客户端
// options: 可选配置（如WithRedisPrefix）
func NewRedisCache(client *redis.Client, prefix string) *RedisCache {
	if prefix == "" {
		prefix = "cache:"
	}
	rc := &RedisCache{
		client: client,
		prefix: prefix, // 默认前缀
	}

	return rc
}

// dataKey 生成数据存储键（前缀+key+":data"）
func (rc *RedisCache) dataKey(key string) string {
	return fmt.Sprintf("%s%s:data", rc.prefix, key)
}

// metaKey 生成元数据存储键（前缀+key+":meta"）
func (rc *RedisCache) metaKey(key string) string {
	return fmt.Sprintf("%s%s:meta", rc.prefix, key)
}

// Put 存入Redis缓存
func (rc *RedisCache) Put(ctx context.Context, key string, value io.Reader, ttl time.Duration) error {
	// 读取全部数据
	data, err := io.ReadAll(value)
	if err != nil {
		return wrapError("read value failed", err)
	}

	// 验证TTL
	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	// 构造元数据
	meta := redisCacheMeta{
		Length:       len(data),
		LastModified: time.Now(),
	}

	// 序列化元数据为JSON
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return wrapError("marshal meta failed", err)
	}

	// 使用Pipeline保证原子性（同时写入数据和元数据）
	pipe := rc.client.Pipeline()
	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	// 设置数据和元数据（永不过期，后续单独设置过期时间）
	pipe.Set(ctx, dataKey, data, 0)
	pipe.Set(ctx, metaKey, metaJSON, 0)

	// 设置过期时间（TTLKeep表示永不过期）
	if ttl != TTLKeep {
		pipe.Expire(ctx, dataKey, ttl)
		pipe.Expire(ctx, metaKey, ttl)
	}

	// 执行Pipeline命令
	_, err = pipe.Exec(ctx)
	if err != nil {
		return wrapError("redis pipeline exec failed", err)
	}

	return nil
}

// Get 从Redis获取缓存
func (rc *RedisCache) Get(ctx context.Context, key string) (*CacheContent, error) {
	// 1. 获取元数据
	metaKey := rc.metaKey(key)
	metaJSON, err := rc.client.Get(ctx, metaKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, wrapError("get meta from redis failed", err)
	}

	// 反序列化元数据
	var meta redisCacheMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, wrapError("unmarshal meta failed", err)
	}

	// 2. 获取数据
	dataKey := rc.dataKey(key)
	data, err := rc.client.Get(ctx, dataKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, wrapError("get data from redis failed", err)
	}

	// 验证数据长度（防止数据损坏）
	if len(data) != meta.Length {
		return nil, fmt.Errorf("cache: data length mismatch (expected %d, got %d)", meta.Length, len(data))
	}

	// 构造可重复读取的CacheContent
	reader := bytes.NewReader(data)
	return &CacheContent{
		ReadSeekCloser: NopCloser{reader},
		Length:         meta.Length,
		LastModified:   meta.LastModified,
	}, nil
}

// Delete 从Redis删除缓存
func (rc *RedisCache) Delete(ctx context.Context, key string) error {
	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	// 批量删除数据和元数据
	_, err := rc.client.Del(ctx, dataKey, metaKey).Result()
	if err != nil {
		return wrapError("redis delete failed", err)
	}
	return nil
}

// Close 关闭Redis缓存（关闭客户端连接池）
func (rc *RedisCache) Close() error {
	return rc.client.Close()
}
