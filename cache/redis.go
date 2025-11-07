package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisCacheMeta struct {
	Length       int       `json:"length"`
	LastModified time.Time `json:"last_modified"`
}

type RedisCache struct {
	client *redis.Client
	prefix string
	closed atomic.Bool
}

func NewRedisCache(client *redis.Client, prefix string) *RedisCache {
	if prefix == "" {
		prefix = "cache:"
	}
	return &RedisCache{
		client: client,
		prefix: prefix,
	}
}

func (rc *RedisCache) isClosed() bool {
	return rc.closed.Load()
}

func (rc *RedisCache) dataKey(key string) string {
	return fmt.Sprintf("%s%s:data", rc.prefix, key)
}

func (rc *RedisCache) metaKey(key string) string {
	return fmt.Sprintf("%s%s:meta", rc.prefix, key)
}

func (rc *RedisCache) Put(ctx context.Context, key string, value io.Reader, ttl time.Duration) error {
	if rc.isClosed() {
		return errors.New("cache is closed")
	}

	data, err := io.ReadAll(value)
	if err != nil {
		return fmt.Errorf("read value: %w", err)
	}

	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	meta := redisCacheMeta{
		Length:       len(data),
		LastModified: time.Now(),
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}

	pipe := rc.client.Pipeline()
	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	pipe.Set(ctx, dataKey, data, 0)
	pipe.Set(ctx, metaKey, metaJSON, 0)

	if ttl != TTLKeep {
		pipe.Expire(ctx, dataKey, ttl)
		pipe.Expire(ctx, metaKey, ttl)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline: %w", err)
	}

	return nil
}

func (rc *RedisCache) Get(ctx context.Context, key string) (*Content, error) {
	if rc.isClosed() {
		return nil, errors.New("cache is closed")
	}

	metaKey := rc.metaKey(key)
	metaJSON, err := rc.client.Get(ctx, metaKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("get meta: %w", err)
	}

	var meta redisCacheMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}

	dataKey := rc.dataKey(key)
	data, err := rc.client.Get(ctx, dataKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, fmt.Errorf("get data: %w", err)
	}

	if len(data) != meta.Length {
		return nil, fmt.Errorf("data length mismatch: expected %d, got %d", meta.Length, len(data))
	}

	reader := bytes.NewReader(data)
	return &Content{
		ReadSeekCloser: NopCloser{reader},
		Length:         meta.Length,
		LastModified:   meta.LastModified,
	}, nil
}

func (rc *RedisCache) Delete(ctx context.Context, key string) error {
	if rc.isClosed() {
		return errors.New("cache is closed")
	}

	dataKey := rc.dataKey(key)
	metaKey := rc.metaKey(key)

	_, err := rc.client.Del(ctx, dataKey, metaKey).Result()
	if err != nil {
		return fmt.Errorf("redis delete: %w", err)
	}
	return nil
}

func (rc *RedisCache) Close() error {
	if rc.closed.CompareAndSwap(false, true) {
		return rc.client.Close()
	}
	return nil
}
