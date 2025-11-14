package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	client *redis.Client
	prefix string
}

func NewRedisCache(client *redis.Client, prefix string) *RedisCache {
	if prefix != "" {
		prefix = strings.Trim(prefix, "/") + "/"
	}
	return &RedisCache{
		client: client,
		prefix: prefix,
	}
}

func (rc *RedisCache) Child(path string) Cache {
	if path == "" {
		return rc
	}
	path = rc.prefix + strings.Trim(path, "/") + "/"
	return &RedisCache{
		client: rc.client,
		prefix: path,
	}
}

func (rc *RedisCache) dataKey(key string) string {
	return fmt.Sprintf("%s%s:data", rc.prefix, key)
}

func (rc *RedisCache) metaKey(key string) string {
	return fmt.Sprintf("%s%s:meta", rc.prefix, key)
}

func (rc *RedisCache) Put(ctx context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error {
	data, err := io.ReadAll(value)
	if err != nil {
		return fmt.Errorf("read value: %w", err)
	}

	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	// 使用事务管道保证原子性
	pipe := rc.client.TxPipeline()
	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	if ttl == TTLKeep {
		// 保持原有TTL - 只设置值，不设置过期时间
		pipe.Set(ctx, dataKey, data, 0)
		pipe.Set(ctx, metaKey, metaJSON, 0)
	} else {
		// 设置新的TTL - 直接在Set命令中设置过期时间
		pipe.Set(ctx, dataKey, data, ttl)
		pipe.Set(ctx, metaKey, metaJSON, ttl)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis transaction: %w", err)
	}

	return nil
}

func (rc *RedisCache) Get(ctx context.Context, key string) (*Content, error) {
	metaKey := rc.metaKey(key)
	dataKey := rc.dataKey(key)

	// 使用事务同时获取元数据和数据，确保一致性
	pipe := rc.client.TxPipeline()
	metaCmd := pipe.Get(ctx, metaKey)
	dataCmd := pipe.Get(ctx, dataKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("redis get: %w", err)
	}

	// 处理元数据
	metaJSON, err := metaCmd.Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("get meta: %w", err)
	}

	var meta map[string]string
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		// 元数据损坏，删除对应的数据键以保持一致性
		go rc.cleanupCorruptedData(context.Background(), dataKey, metaKey)
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}

	// 处理数据
	data, err := dataCmd.Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 数据不存在但元数据存在，删除元数据以保持一致性
			go rc.cleanupCorruptedData(context.Background(), dataKey, metaKey)
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("get data: %w", err)
	}

	reader := bytes.NewReader(data)
	return &Content{
		ReadSeekCloser: NopCloser{reader},
		Metadata:       meta,
	}, nil
}

// cleanupCorruptedData 清理损坏的数据，用于后台修复数据不一致
func (rc *RedisCache) cleanupCorruptedData(ctx context.Context, dataKey, metaKey string) {
	_, _ = rc.client.Del(ctx, dataKey, metaKey).Result()
}

func (rc *RedisCache) Delete(ctx context.Context, key string) error {
	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	_, err := rc.client.Del(ctx, dataKey, metaKey).Result()
	if err != nil {
		return fmt.Errorf("redis delete: %w", err)
	}
	return nil
}

func (rc *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	metaKey := rc.metaKey(key)
	exists, err := rc.client.Exists(ctx, metaKey).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}
	return exists > 0, nil
}

func (rc *RedisCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	dataKey := rc.dataKey(key)
	ttl, err := rc.client.TTL(ctx, dataKey).Result()
	if err != nil {
		return 0, fmt.Errorf("redis ttl: %w", err)
	}
	return ttl, nil
}
