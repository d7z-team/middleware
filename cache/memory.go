package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"
)

type NopCloser struct {
	io.ReadSeeker
}

func (NopCloser) Close() error { return nil }

type internalValue struct {
	data      []byte
	length    uint64
	metadata  []byte
	expiresAt time.Time
}

type MemoryCache struct {
	lruCache   *lru.Cache[string, *internalValue]
	cleanupInt time.Duration

	mu     sync.RWMutex
	closed *atomic.Bool
	stopWG sync.WaitGroup

	prefix string
}

type MemoryCacheConfig struct {
	MaxCapacity int
	CleanupInt  time.Duration
}

func NewMemoryCache(config MemoryCacheConfig) (*MemoryCache, error) {
	if config.MaxCapacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	if config.CleanupInt <= 0 {
		config.CleanupInt = 5 * time.Minute
	}

	lruCache, err := lru.New[string, *internalValue](config.MaxCapacity)
	if err != nil {
		return nil, errors.Wrap(err, "create lru cache failed")
	}

	mc := &MemoryCache{
		lruCache:   lruCache,
		cleanupInt: config.CleanupInt,
		closed:     new(atomic.Bool),
		prefix:     "/",
	}

	mc.stopWG.Add(1)
	go mc.startCleanupTask()

	return mc, nil
}

func (mc *MemoryCache) Child(paths ...string) Cache {
	if len(paths) == 0 {
		return mc
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
		return mc
	}

	return &MemoryCache{
		lruCache: mc.lruCache,
		closed:   mc.closed,
		prefix:   mc.prefix + strings.Join(keys, "/") + "/",
	}
}

func (mc *MemoryCache) Put(_ context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	key = mc.prefix + key
	data, err := io.ReadAll(value)
	if err != nil {
		return errors.Wrap(err, "read value failed")
	}

	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	now := time.Now()
	expiresAt := time.Time{}
	if ttl != TTLKeep {
		expiresAt = now.Add(ttl)
	}
	metaRaw, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "marshal metadata failed")
	}
	internalVal := &internalValue{
		data:      data,
		length:    uint64(len(data)),
		metadata:  metaRaw,
		expiresAt: expiresAt,
	}

	mc.mu.Lock()
	mc.lruCache.Add(key, internalVal)
	mc.mu.Unlock()

	return nil
}

func (mc *MemoryCache) Get(_ context.Context, key string) (*Content, error) {
	if mc.closed.Load() {
		return nil, ErrCacheClosed
	}

	key = mc.prefix + key
	mc.mu.RLock()
	defer mc.mu.RUnlock()

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

func (mc *MemoryCache) Delete(_ context.Context, key string) error {
	if mc.closed.Load() {
		return ErrCacheClosed
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
