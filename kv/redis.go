package kv

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisKV is a Redis-based KV storage implementation.
type RedisKV struct {
	client *redis.Client
	prefix string // key prefix
}

// Count counts the number of keys matching the prefix.
// Note: This operation uses SCAN and can be slow for large datasets.
func (r *RedisKV) Count(ctx context.Context) (int64, error) {
	var count int64
	fullPattern := r.prefix + "*"
	cursor := uint64(0)
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, fullPattern, 1000).Result()
		if err != nil {
			return 0, err
		}

		count += int64(len(keys))
		if cursor == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
	}

	return count, nil
}

// NewRedis creates a new Redis KV instance.
func NewRedis(client *redis.Client, prefix string) *RedisKV {
	return &RedisKV{
		client: client,
		prefix: prefix,
	}
}

// Child creates a child KV with appended path.
func (r *RedisKV) Child(paths ...string) KV {
	if len(paths) == 0 {
		return r
	}
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		keys = append(keys, path)
	}
	if len(keys) == 0 {
		return r
	}
	return &RedisKV{
		client: r.client,
		prefix: r.prefix + strings.Join(keys, "/") + "/",
	}
}

// Put stores a key-value pair.
func (r *RedisKV) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	fullKey := r.prefix + key
	if ttl == TTLKeep {
		return r.client.Set(ctx, fullKey, value, redis.KeepTTL).Err()
	}
	return r.client.Set(ctx, fullKey, value, ttl).Err()
}

// Get retrieves the value for a key. Returns ErrKeyNotFound if not found.
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

// Delete removes a key.
func (r *RedisKV) Delete(ctx context.Context, key string) (bool, error) {
	fullKey := r.prefix + key
	delCount, err := r.client.Del(ctx, fullKey).Result()
	if err != nil {
		return false, err
	}
	return delCount > 0, nil
}

// PutIfNotExists sets the value only if the key does not exist.
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

// CompareAndSwap updates the value if it matches the old value.
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
			pipe.Set(ctx, fullKey, newValue, redis.KeepTTL)
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

// CursorList implements cursor-based pagination.
// Note: Redis SCAN does not support lexicographical order.
// To ensure consistency with other implementations, this method fetches all keys matching the prefix,
// sorts them, and then applies the cursor/limit logic.
// This is inefficient for large datasets but necessary for strict interface compliance regarding ordering.
func (r *RedisKV) CursorList(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := &ListOptions{}
	if options != nil {
		opts = options
	}
	if opts.Limit == 0 {
		opts.Limit = 1000
	}

	// Fetch all keys matching prefix
	// We re-use List which returns map[string]string, but we only need keys.
	// Optimization: Implement a "ListKeys" internal method or just do scanning here.
	fullPrefix := r.prefix
	keys := make([]string, 0)
	cursor := uint64(0)

	for {
		res, nextCursor, err := r.client.Scan(ctx, cursor, fullPrefix+"*", 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("scan keys failed: %w", err)
		}
		keys = append(keys, res...)
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Filter keys that don't match prefix (SCAN pattern is glob-style, pretty accurate, but let's be safe)
	// And strip prefix
	filteredKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, r.prefix) {
			filteredKeys = append(filteredKeys, k[len(r.prefix):])
		}
	}

	// Sort lexicographically
	sort.Strings(filteredKeys)

	// Apply Cursor
	startIndex := 0
	if opts.Cursor != "" {
		for i, key := range filteredKeys {
			if key > opts.Cursor {
				startIndex = i
				break
			}
		}
		// If cursor is beyond all keys (or matches the last one), startIndex might be 0 but logic below handles it
		// If no key > cursor, startIndex should be len
		if startIndex == 0 && (len(filteredKeys) == 0 || filteredKeys[0] <= opts.Cursor) {
			// Check if we actually found a key > cursor
			found := false
			for i, key := range filteredKeys {
				if key > opts.Cursor {
					startIndex = i
					found = true
					break
				}
			}
			if !found {
				startIndex = len(filteredKeys)
			}
		}
	}

	// Apply Limit
	endIndex := startIndex + int(opts.Limit)
	if endIndex > len(filteredKeys) {
		endIndex = len(filteredKeys)
	}

	resultKeys := filteredKeys[startIndex:endIndex]

	// Determine next cursor
	var nextCursor string
	hasMore := endIndex < len(filteredKeys)
	if len(resultKeys) > 0 {
		// Next cursor is the last key returned
		// Wait, typical cursor pagination: pass the last key you saw.
		// Next page starts after that.
		// So HasMore is true if there are items after resultKeys
	}
	if hasMore {
		nextCursor = resultKeys[len(resultKeys)-1]
	}

	return &ListResponse{
		Keys:    resultKeys,
		Cursor:  nextCursor,
		HasMore: hasMore,
	}, nil
}

// List retrieves all key-value pairs matching the prefix.
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

// ListPage retrieves paginated key-value pairs matching the prefix.
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
