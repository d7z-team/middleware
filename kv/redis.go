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

func (r *RedisKV) buildKey(key string) (string, error) {
	if strings.Contains(key, "/") {
		return "", ErrInvalidKey
	}
	return r.prefix + key, nil
}

// Put stores a key-value pair.
func (r *RedisKV) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	fullKey, err := r.buildKey(key)
	if err != nil {
		return err
	}
	if ttl == TTLKeep {
		return r.client.Set(ctx, fullKey, value, redis.KeepTTL).Err()
	}
	return r.client.Set(ctx, fullKey, value, ttl).Err()
}

// Get retrieves the value for a key. Returns ErrKeyNotFound if not found.
func (r *RedisKV) Get(ctx context.Context, key string) (string, error) {
	fullKey, err := r.buildKey(key)
	if err != nil {
		return "", err
	}
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
	fullKey, err := r.buildKey(key)
	if err != nil {
		return false, err
	}
	delCount, err := r.client.Del(ctx, fullKey).Result()
	if err != nil {
		return false, err
	}
	return delCount > 0, nil
}

// DeleteAll removes all keys under the current prefix.
func (r *RedisKV) DeleteAll(ctx context.Context) error {
	fullPattern := r.prefix + "*"
	cursor := uint64(0)
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, fullPattern, 1000).Result()
		if err != nil {
			return fmt.Errorf("scan keys failed: %w", err)
		}

		if len(keys) > 0 {
			if err := r.client.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("delete keys failed: %w", err)
			}
		}

		if cursor == 0 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// PutIfNotExists sets the value only if the key does not exist.
func (r *RedisKV) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if invalidTTL(ttl) {
		return false, ErrInvalidTTL
	}
	fullKey, err := r.buildKey(key)
	if err != nil {
		return false, err
	}
	var cmd *redis.BoolCmd

	pipe := r.client.Pipeline()
	defer pipe.Close()

	if ttl == TTLKeep {
		cmd = pipe.SetNX(ctx, fullKey, value, 0)
	} else {
		cmd = pipe.SetNX(ctx, fullKey, value, ttl)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	return cmd.Val(), nil
}

// CompareAndSwap updates the value if it matches the old value.
func (r *RedisKV) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	fullKey, err := r.buildKey(key)
	if err != nil {
		return false, err
	}

	if err := r.client.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, fullKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return ErrCASFailed
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

func (r *RedisKV) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	keys := make([]string, 0)
	cursor := uint64(0)
	for {
		res, nextCursor, err := r.client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return nil, err
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
	return keys, nil
}

func (r *RedisKV) fetchPairs(ctx context.Context, fullKeys []string) ([]Pair, error) {
	if len(fullKeys) == 0 {
		return []Pair{}, nil
	}
	values, err := r.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, err
	}
	pairs := make([]Pair, 0, len(fullKeys))
	for i, fullKey := range fullKeys {
		bizKey := fullKey[len(r.prefix):]
		val, _ := values[i].(string)
		pairs = append(pairs, Pair{Key: bizKey, Value: val})
	}
	return pairs, nil
}

// ListCursor implements cursor-based pagination.
func (r *RedisKV) ListCursor(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := normalizeListOptions(options)
	res, err := r.Scan(ctx, ScanOptions{
		Cursor: opts.Cursor,
		Limit:  int(opts.Limit),
	})
	if err != nil {
		return nil, err
	}
	return &ListResponse{
		Pairs:   res.Pairs,
		Cursor:  res.NextCursor,
		HasMore: res.HasMore,
	}, nil
}

// List retrieves all key-value pairs matching the prefix.
func (r *RedisKV) List(ctx context.Context, prefix string) ([]Pair, error) {
	// For small to medium datasets, we can use Scan with a large limit.
	// This ensures we use the batching logic of Scan.
	res, err := r.Scan(ctx, ScanOptions{
		Prefix: prefix,
		Limit:  1000000,
	})
	if err != nil {
		return nil, err
	}
	return res.Pairs, nil
}

// ListCurrent returns key-value pairs at the current level.
func (r *RedisKV) ListCurrent(ctx context.Context, prefix string) ([]Pair, error) {
	keys, err := r.scanKeys(ctx, r.prefix+prefix+"*")
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	filtered := make([]string, 0)
	for _, k := range keys {
		if isCurrentLevel(k[len(r.prefix):], prefix) {
			filtered = append(filtered, k)
		}
	}
	return r.fetchPairs(ctx, filtered)
}

// ListPage retrieves paginated key-value pairs matching the prefix.
func (r *RedisKV) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := r.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	start, end := listPageRange(len(all), pageIndex, pageSize)
	return all[start:end], nil
}

// ListCurrentPage returns a page of key-value pairs at the current level.
func (r *RedisKV) ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := r.ListCurrent(ctx, prefix)
	if err != nil {
		return nil, err
	}
	start, end := listPageRange(len(all), pageIndex, pageSize)
	return all[start:end], nil
}

// ListCurrentCursor implements cursor-based pagination for current level.
func (r *RedisKV) ListCurrentCursor(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := normalizeListOptions(options)
	keys, err := r.scanKeys(ctx, r.prefix+"*")
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0)
	for _, k := range keys {
		rel := k[len(r.prefix):]
		if isCurrentLevel(rel, "") {
			pairs = append(pairs, Pair{Key: rel})
		}
	}

	start := listCursorStartIndex(pairs, opts.Cursor)
	end := start + int(opts.Limit)
	if end > len(pairs) {
		end = len(pairs)
	}

	p := pairs[start:end]
	fullKeys := make([]string, 0, len(p))
	for _, item := range p {
		fullKeys = append(fullKeys, r.prefix+item.Key)
	}

	resPairs, err := r.fetchPairs(ctx, fullKeys)
	if err != nil {
		return nil, err
	}

	hasMore := end < len(pairs)
	var nextCursor string
	if hasMore {
		nextCursor = resPairs[len(resPairs)-1].Key
	}
	return &ListResponse{Pairs: resPairs, Cursor: nextCursor, HasMore: hasMore}, nil
}

// Scan performs a prefix scan with pagination support.
func (r *RedisKV) Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	fullPattern := r.prefix + opts.Prefix + "*"
	keys, err := r.scanKeys(ctx, fullPattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, Pair{Key: k[len(r.prefix):]})
	}

	start := listCursorStartIndex(pairs, opts.Cursor)
	end := start + opts.Limit
	if end > len(pairs) {
		end = len(pairs)
	}

	resultPairs := pairs[start:end]
	fullKeys := make([]string, 0, len(resultPairs))
	for _, p := range resultPairs {
		fullKeys = append(fullKeys, r.prefix+p.Key)
	}

	resPairs, err := r.fetchPairs(ctx, fullKeys)
	if err != nil {
		return nil, err
	}

	hasMore := end < len(pairs)
	var nextCursor string
	if hasMore {
		nextCursor = resPairs[len(resPairs)-1].Key
	}

	return &ScanResponse{
		Pairs:      resPairs,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// PutBatch stores multiple key-value pairs using a pipeline.
func (r *RedisKV) PutBatch(ctx context.Context, pairs []Pair, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	pipe := r.client.Pipeline()
	for _, p := range pairs {
		fullKey, err := r.buildKey(p.Key)
		if err != nil {
			return err
		}
		if ttl == TTLKeep {
			pipe.Set(ctx, fullKey, p.Value, redis.KeepTTL)
		} else {
			pipe.Set(ctx, fullKey, p.Value, ttl)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

// GetBatch retrieves values for multiple keys using MGET.
func (r *RedisKV) GetBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fk, err := r.buildKey(k)
		if err != nil {
			return nil, err
		}
		fullKeys[i] = fk
	}

	vals, err := r.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, err
	}

	res := make([]string, len(vals))
	for i, v := range vals {
		if v == nil {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, keys[i])
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for key %s", keys[i])
		}
		res[i] = s
	}
	return res, nil
}

// DeleteBatch removes multiple keys using a single DEL command.
func (r *RedisKV) DeleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fk, err := r.buildKey(k)
		if err != nil {
			return err
		}
		fullKeys[i] = fk
	}
	return r.client.Del(ctx, fullKeys...).Err()
}
