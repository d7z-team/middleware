package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
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
	maxCap     int
	cleanupInt time.Duration
	stopChan   chan struct{}
	once       sync.Once
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
		return nil, wrapError("create lru cache failed", err)
	}

	mc := &MemoryCache{
		lruCache:   lruCache,
		maxCap:     config.MaxCapacity,
		cleanupInt: config.CleanupInt,
		stopChan:   make(chan struct{}),
	}

	go mc.startCleanupTask()

	return mc, nil
}

func (mc *MemoryCache) Put(_ context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error {
	data, err := io.ReadAll(value)
	if err != nil {
		return wrapError("read value failed", err)
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
		return wrapError("marshal metadata failed", err)
	}
	internalVal := &internalValue{
		data:      data,
		length:    uint64(len(data)),
		metadata:  metaRaw,
		expiresAt: expiresAt,
	}

	mc.lruCache.Add(key, internalVal)

	return nil
}

func (mc *MemoryCache) Get(_ context.Context, key string) (*Content, error) {
	internalVal, exists := mc.lruCache.Get(key)
	if !exists {
		return nil, ErrCacheMiss
	}

	now := time.Now()
	if !internalVal.expiresAt.IsZero() && now.After(internalVal.expiresAt) {
		mc.lruCache.Remove(key)
		return nil, ErrCacheMiss
	}

	reader := bytes.NewReader(internalVal.data)
	meta := make(map[string]string)
	err := json.Unmarshal(internalVal.metadata, &meta)
	if err != nil {
		return nil, wrapError("unmarshal metadata failed", err)
	}
	return &Content{
		ReadSeekCloser: NopCloser{reader},
		Metadata:       meta,
	}, nil
}

func (mc *MemoryCache) Delete(_ context.Context, key string) error {
	mc.lruCache.Remove(key)
	return nil
}

func (mc *MemoryCache) Close() error {
	mc.once.Do(func() {
		close(mc.stopChan)
	})
	return nil
}

func (mc *MemoryCache) startCleanupTask() {
	ticker := time.NewTicker(mc.cleanupInt)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.cleanupExpired()
		case <-mc.stopChan:
			return
		}
	}
}

func (mc *MemoryCache) cleanupExpired() {
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

type cacheError struct {
	msg string
	err error
}

func (e *cacheError) Error() string {
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

func (e *cacheError) Unwrap() error { return e.err }

func wrapError(msg string, err error) error {
	return &cacheError{msg: msg, err: err}
}
