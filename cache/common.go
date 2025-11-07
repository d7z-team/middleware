package cache

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

const TTLKeep = -1

type Cache interface {
	Put(ctx context.Context, key string, value io.Reader, ttl time.Duration) error
	Get(ctx context.Context, key string) (*Content, error)
	Delete(ctx context.Context, key string) error
	io.Closer
}

type Content struct {
	io.ReadSeekCloser
	Length       int
	LastModified time.Time
}

func (c *Content) ReadToString() (string, error) {
	all, err := io.ReadAll(c)
	if err != nil {
		return "", err
	}
	_, _ = c.Seek(0, io.SeekStart)
	return string(all), nil
}

// 通用缓存错误定义
var (
	// ErrCacheMiss 缓存未命中或已过期
	ErrCacheMiss = errors.Join(os.ErrNotExist, errors.New("cache: key not found or expired"))
	// ErrInvalidTTL 无效的TTL值（非TTLKeep且小于等于0）
	ErrInvalidTTL = errors.New("cache: invalid ttl value (use TTLKeep for permanent cache)")
	// ErrInvalidCapacity 无效的缓存容量（小于等于0）
	ErrInvalidCapacity = errors.New("cache: invalid cache capacity (must be greater than 0)")
)

// NewCacheFromURL 通过 URL 配置创建缓存实例
// 支持的协议方案：
// - memory/mem: 内存缓存（带LRU），示例：memory://?max_capacity=1000&cleanup_interval=5m
// - redis: Redis 缓存（非TLS），示例：redis://:password@localhost:6379/0?prefix=app:cache:
// - rediss: Redis 缓存（TLS），示例：rediss://user:password@host:6380/1?prefix=cache:
func NewCacheFromURL(u string) (Cache, error) {
	// 解析 URL
	ur, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	// 根据协议方案创建对应缓存实例
	switch strings.ToLower(ur.Scheme) {
	case "memory", "mem":
		// 内存缓存：解析查询参数配置
		return newMemoryCacheFromURL(ur)
	case "redis", "rediss":
		client, err := connects.NewRedis(ur)
		if err != nil {
			return nil, err
		}
		return NewRedisCache(client, ur.Query().Get("prefix")), nil

	default:
		return nil, errors.New("不支持的缓存协议: " + ur.Scheme)
	}
}

func newMemoryCacheFromURL(ur *url.URL) (*MemoryCache, error) {
	query := ur.Query()

	// 1. 解析最大容量（必填）
	maxCap := 10 * 1024 * 1024
	var err error
	maxCapStr := query.Get("max_capacity")
	if maxCapStr != "" {
		maxCap, err = strconv.Atoi(maxCapStr)
		if err != nil || maxCap <= 0 {
			return nil, errors.New("max_capacity 必须是正整数")
		}
	}

	cleanupIntervalStr := query.Get("cleanup_interval")
	cleanupInterval := 5 * time.Minute
	if cleanupIntervalStr != "" {
		dur, err := time.ParseDuration(cleanupIntervalStr)
		if err != nil {
			return nil, errors.New("cleanup_interval 格式无效（支持s/m/h，如5m）: " + err.Error())
		}
		cleanupInterval = dur
	}

	return NewMemoryCache(
		MemoryCacheConfig{
			MaxCapacity: maxCap,
			CleanupInt:  cleanupInterval,
		},
	)
}
