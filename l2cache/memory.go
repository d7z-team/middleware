package l2cache

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// NopCloser 空关闭实现（与原接口一致）
type NopCloser struct {
	io.ReadSeeker
}

func (NopCloser) Close() error { return nil }

// internalValue 内部缓存值结构（封装原 lruNode 的核心字段）
type internalValue struct {
	data       []byte    // 缓存数据
	length     int       // 数据长度
	lastModify time.Time // 最后修改时间
	expiresAt  time.Time // 过期时间（零值=永不过期）
}

// MemoryCache 基于 golang-lru 的内存缓存（保持原接口一致）
type MemoryCache struct {
	lruCache   *lru.Cache[string, *internalValue] // 底层 LRU 缓存（hashicorp 实现）
	maxCap     int                                // 最大缓存容量（节点数）
	cleanupInt time.Duration                      // 过期清理间隔
	stopChan   chan struct{}                      // 停止信号通道
	once       sync.Once                          // 确保 Close 幂等
}

// MemoryCacheOption 缓存配置选项（与原接口一致）
type MemoryCacheOption func(*MemoryCache) error

// WithMaxCapacity 设置最大容量（必须 > 0）
func WithMaxCapacity(capacity int) MemoryCacheOption {
	return func(mc *MemoryCache) error {
		if capacity <= 0 {
			return ErrInvalidCapacity
		}
		mc.maxCap = capacity
		return nil
	}
}

// WithCleanupInterval 设置过期清理间隔（<=0 用默认 5 分钟）
func WithCleanupInterval(interval time.Duration) MemoryCacheOption {
	return func(mc *MemoryCache) error {
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		mc.cleanupInt = interval
		return nil
	}
}

// NewMemoryCache 创建缓存实例（与原接口一致）
func NewMemoryCache(options ...MemoryCacheOption) (*MemoryCache, error) {
	mc := &MemoryCache{
		cleanupInt: 5 * time.Minute,
		stopChan:   make(chan struct{}),
	}

	// 应用配置选项
	for _, opt := range options {
		if err := opt(mc); err != nil {
			return nil, err
		}
	}

	// 校验容量（必须设置有效容量）
	if mc.maxCap <= 0 {
		return nil, ErrInvalidCapacity
	}

	// 创建 hashicorp LRU 缓存（并发安全由底层实现保证）
	lruCache, err := lru.New[string, *internalValue](mc.maxCap)
	if err != nil {
		return nil, wrapError("create lru cache failed", err)
	}
	mc.lruCache = lruCache

	// 启动后台过期清理任务
	go mc.startCleanupTask()

	return mc, nil
}

// Put 存入缓存（支持 TTL 和 LRU 淘汰，与原接口一致）
func (mc *MemoryCache) Put(_ context.Context, key string, value io.Reader, ttl time.Duration) error {
	// 读取输入数据
	data, err := io.ReadAll(value)
	if err != nil {
		return wrapError("read value failed", err)
	}

	// 校验 TTL
	if ttl != TTLKeep && ttl <= 0 {
		return ErrInvalidTTL
	}

	// 计算过期时间
	now := time.Now()
	expiresAt := time.Time{}
	if ttl != TTLKeep {
		expiresAt = now.Add(ttl)
	}

	// 构建内部缓存值（复用或更新已有节点）
	internalVal := &internalValue{
		data:       data,
		length:     len(data),
		lastModify: now,
		expiresAt:  expiresAt,
	}

	// 存入 LRU 缓存（底层自动处理：存在则更新+移到队首，满则淘汰最少访问）
	mc.lruCache.Add(key, internalVal)

	return nil
}

// Get 获取缓存（命中更新 LRU 顺序，过期自动删除，与原接口一致）
func (mc *MemoryCache) Get(_ context.Context, key string) (*CacheContent, error) {
	// 从 LRU 缓存获取（底层自动更新访问顺序）
	internalVal, exists := mc.lruCache.Get(key)
	if !exists {
		return nil, ErrCacheMiss
	}

	// 检查是否过期
	now := time.Now()
	if !internalVal.expiresAt.IsZero() && now.After(internalVal.expiresAt) {
		// 过期则删除缓存
		mc.lruCache.Remove(key)
		return nil, ErrCacheMiss
	}

	// 封装为可重复读取的 CacheContent（与原接口一致）
	reader := bytes.NewReader(internalVal.data)
	return &CacheContent{
		ReadSeekCloser: NopCloser{reader},
		Length:         internalVal.length,
		LastModified:   internalVal.lastModify,
	}, nil
}

// Delete 删除指定缓存键（与原接口一致）
func (mc *MemoryCache) Delete(_ context.Context, key string) error {
	mc.lruCache.Remove(key)
	return nil
}

// Close 关闭缓存（停止后台任务，与原接口一致）
func (mc *MemoryCache) Close() error {
	mc.once.Do(func() {
		close(mc.stopChan)
	})
	return nil
}

// startCleanupTask 后台过期清理任务（定期遍历并删除过期项）
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

// cleanupExpired 清理所有过期缓存项
func (mc *MemoryCache) cleanupExpired() {
	now := time.Now()
	// 遍历所有缓存键（Keys() 返回当前键的快照，线程安全）
	for _, key := range mc.lruCache.Keys() {
		// 再次检查键是否存在（避免遍历期间被删除）
		internalVal, exists := mc.lruCache.Get(key)
		if !exists {
			continue
		}
		// 过期则删除
		if !internalVal.expiresAt.IsZero() && now.After(internalVal.expiresAt) {
			mc.lruCache.Remove(key)
		}
	}
}

// -------------- 错误处理（与原实现一致）--------------
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
