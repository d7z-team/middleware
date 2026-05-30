package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"
	"gopkg.d7z.net/middleware/utils"
)

type NopCloser struct {
	io.ReadSeeker
}

func (NopCloser) Close() error { return nil }

type internalValue struct {
	data      []byte
	metadata  []byte
	size      uint64
	expiresAt time.Time
}

type MemoryCache struct {
	lruCache   *lru.Cache[string, *internalValue]
	cleanupInt time.Duration

	mu     *sync.RWMutex
	closed *atomic.Bool
	stopWG *sync.WaitGroup

	prefix    string
	maxBytes  uint64
	usedBytes *uint64
}

type MemoryCacheConfig struct {
	MaxCapacity int
	MaxBytes    uint64
	CleanupInt  time.Duration
}

// NewMemoryCache creates an in-memory cache with LRU eviction.
//
// Example:
//
//	cache, _ := NewMemoryCache(MemoryCacheConfig{MaxCapacity: 1024})
func NewMemoryCache(config MemoryCacheConfig) (*MemoryCache, error) {
	if config.MaxCapacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	if config.CleanupInt <= 0 {
		config.CleanupInt = 5 * time.Minute
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = defaultMemoryCacheMaxBytes()
	}

	usedBytes := new(uint64)
	lruCache, err := lru.NewWithEvict[string, *internalValue](config.MaxCapacity, func(_ string, value *internalValue) {
		if value != nil {
			*usedBytes -= value.size
		}
	})
	if err != nil {
		return nil, errors.Wrap(err, "create lru cache failed")
	}

	mc := &MemoryCache{
		lruCache:   lruCache,
		cleanupInt: config.CleanupInt,
		mu:         &sync.RWMutex{},
		closed:     new(atomic.Bool),
		stopWG:     &sync.WaitGroup{},
		prefix:     "/",
		maxBytes:   config.MaxBytes,
		usedBytes:  usedBytes,
	}

	mc.stopWG.Add(1)
	go mc.startCleanupTask()

	return mc, nil
}

func (mc *MemoryCache) Child(paths ...string) Cache {
	childPath := utils.MustChild(paths...)
	if childPath == "" {
		return mc
	}

	return &MemoryCache{
		lruCache:  mc.lruCache,
		mu:        mc.mu,
		closed:    mc.closed,
		stopWG:    mc.stopWG,
		prefix:    mc.prefix + childPath + "/",
		maxBytes:  mc.maxBytes,
		usedBytes: mc.usedBytes,
	}
}

func (mc *MemoryCache) Put(ctx context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	metaRaw, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "marshal metadata failed")
	}
	if uint64(len(metaRaw)) > mc.maxBytes {
		return ErrCacheTooLarge
	}

	dataLimit := mc.maxBytes - uint64(len(metaRaw))
	reader := value
	if dataLimit < maxInt64Uint {
		reader = io.LimitReader(value, int64(dataLimit)+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if uint64(len(data)) > dataLimit {
		return ErrCacheTooLarge
	}

	key = mc.prefix + key
	now := time.Now()
	expiresAt := time.Time{}
	if ttl != TTLKeep {
		expiresAt = now.Add(ttl)
	}
	internalVal := &internalValue{
		data:      data,
		metadata:  metaRaw,
		size:      uint64(len(data)) + uint64(len(metaRaw)),
		expiresAt: expiresAt,
	}

	mc.mu.Lock()
	if existing, exists := mc.lruCache.Peek(key); exists && existing != nil {
		*mc.usedBytes -= existing.size
	}
	*mc.usedBytes += internalVal.size
	mc.lruCache.Add(key, internalVal)
	for *mc.usedBytes > mc.maxBytes {
		if _, _, ok := mc.lruCache.RemoveOldest(); !ok {
			break
		}
	}
	mc.mu.Unlock()

	return nil
}

func (mc *MemoryCache) Get(ctx context.Context, key string) (*Content, error) {
	if mc.closed.Load() {
		return nil, ErrCacheClosed
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	key = mc.prefix + key
	mc.mu.Lock()
	defer mc.mu.Unlock()

	internalVal, exists := mc.lruCache.Get(key)
	if !exists {
		return nil, ErrCacheMiss
	}

	now := time.Now()
	if !internalVal.expiresAt.IsZero() && now.After(internalVal.expiresAt) {
		return nil, ErrCacheMiss
	}

	reader := bytes.NewReader(internalVal.data)
	meta := make(map[string]string)
	err := json.Unmarshal(internalVal.metadata, &meta)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal metadata failed")
	}
	return &Content{
		ReadSeekCloser: NopCloser{reader},
		Metadata:       meta,
	}, nil
}

func (mc *MemoryCache) Delete(ctx context.Context, key string) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	key = mc.prefix + key
	mc.mu.Lock()
	mc.lruCache.Remove(key)
	mc.mu.Unlock()

	return nil
}

func (mc *MemoryCache) Close() error {
	if mc.closed.CompareAndSwap(false, true) {
		mc.stopWG.Wait()
	}
	return nil
}

func (mc *MemoryCache) startCleanupTask() {
	defer mc.stopWG.Done()

	ticker := time.NewTicker(mc.cleanupInt)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.cleanupExpired()
		default:
			if mc.closed.Load() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (mc *MemoryCache) cleanupExpired() {
	if mc.closed.Load() {
		return
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	now := time.Now()
	for _, key := range mc.lruCache.Keys() {
		internalVal, exists := mc.lruCache.Get(key)
		if !exists {
			continue
		}
		if !internalVal.expiresAt.IsZero() && now.After(internalVal.expiresAt) {
			mc.lruCache.Remove(key)
		}
	}
}
