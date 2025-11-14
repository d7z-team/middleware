package kv

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisKV struct {
	client *redis.Client
	prefix string
}

func NewRedis(client *redis.Client, prefix string) *RedisKV {
	return &RedisKV{
		client: client,
		prefix: prefix,
	}
}

func (r *RedisKV) Child(path string) KV {
	path = strings.Trim(path, "/") + "/"
	if path == "" {
		return r
	}
	return &RedisKV{
		client: r.client,
		prefix: r.prefix + path,
	}
}

func (r *RedisKV) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	fullKey := r.prefix + key
	if ttl == TTLKeep {
		return r.client.Set(ctx, fullKey, value, 0).Err()
	}
	return r.client.Set(ctx, fullKey, value, ttl).Err()
}

func (r *RedisKV) Get(ctx context.Context, key string) (string, error) {
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

func (r *RedisKV) Delete(ctx context.Context, key string) (bool, error) {
	fullKey := r.prefix + key
	delCount, err := r.client.Del(ctx, fullKey).Result()
	if err != nil {
		return false, err
	}
	return delCount > 0, nil
}

func (r *RedisKV) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	fullKey := r.prefix + key
	var cmd *redis.BoolCmd

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

func (r *RedisKV) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	fullKey := r.prefix + key

	if err := r.client.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, fullKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return ErrKeyNotFound
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

	return true, nil
}

func (r *RedisKV) List(ctx context.Context, prefix string) (map[string]string, error) {
	fullPrefix := r.prefix + prefix
	keys := make([]string, 0)
	cursor := uint64(0)

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

	values, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(keys))
	for i, fullKey := range keys {
		bizKey := fullKey[len(r.prefix):]
		val, ok := values[i].(string)
		if ok && val != "" {
			result[bizKey] = val
		}
	}

	return result, nil
}

func (r *RedisKV) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	fullList, err := r.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fullList))
	for k := range fullList {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	start := pageIndex * uint64(pageSize)
	if start >= uint64(len(keys)) {
		return make(map[string]string), nil
	}
	end := start + uint64(pageSize)
	if end > uint64(len(keys)) {
		end = uint64(len(keys))
	}

	pageKeys := keys[start:end]
	pageResult := make(map[string]string, len(pageKeys))
	for _, k := range pageKeys {
		pageResult[k] = fullList[k]
	}
	return pageResult, nil
}
