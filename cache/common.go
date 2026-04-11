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
	Child(paths ...string) Cache
	Put(ctx context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error
	Get(ctx context.Context, key string) (*Content, error)
	Delete(ctx context.Context, key string) error
}

type CloserCache interface {
	Cache
	io.Closer
}

type closerCache struct {
	Cache
	closer func() error
}

func (c closerCache) Close() error {
	return c.closer()
}

type Content struct {
	io.ReadSeekCloser
	Metadata map[string]string
}

// ReadToString reads the content and rewinds it before returning.
//
// Example:
//
//	content, _ := cache.Get(ctx, "avatar")
//	text, _ := content.ReadToString()
func (c *Content) ReadToString() (string, error) {
	all, err := io.ReadAll(c)
	if err != nil {
		return "", err
	}
	_, _ = c.Seek(0, io.SeekStart)
	return string(all), nil
}

var (
	ErrCacheMiss       = errors.Join(os.ErrNotExist, errors.New("cache: key not found or expired"))
	ErrInvalidTTL      = errors.New("cache: invalid ttl value (use TTLKeep for permanent cache)")
	ErrInvalidCapacity = errors.New("cache: invalid cache capacity (must be greater than 0)")
	ErrCacheClosed     = errors.New("cache is closed")
)

// NewCacheFromURL creates a cache from a connection URL.
//
// Example:
//
//	cache, _ := NewCacheFromURL("memory://?max_capacity=1000&cleanup_interval=5m")
func NewCacheFromURL(u string) (CloserCache, error) {
	ur, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(ur.Scheme) {
	case "memory", "mem":
		mem, err := newMemoryCacheFromURL(ur)
		if err != nil {
			return nil, err
		}
		return closerCache{
			Cache:  mem,
			closer: mem.Close,
		}, nil
	case "redis", "rediss":
		client, err := connects.NewRedis(ur)
		if err != nil {
			return nil, err
		}
		return closerCache{
			Cache: NewRedisCache(client, ur.Query().Get("prefix")),
			closer: func() error {
				return client.Close()
			},
		}, nil

	default:
		return nil, errors.New("unsupported cache scheme: " + ur.Scheme)
	}
}

func newMemoryCacheFromURL(ur *url.URL) (*MemoryCache, error) {
	query := ur.Query()

	maxCap := 10 * 1024 * 1024
	var err error
	maxCapStr := query.Get("max_capacity")
	if maxCapStr != "" {
		maxCap, err = strconv.Atoi(maxCapStr)
		if err != nil || maxCap <= 0 {
			return nil, errors.New("max_capacity must be a positive integer")
		}
	}

	cleanupIntervalStr := query.Get("cleanup_interval")
	cleanupInterval := 5 * time.Minute
	if cleanupIntervalStr != "" {
		dur, err := time.ParseDuration(cleanupIntervalStr)
		if err != nil {
			return nil, errors.New("invalid cleanup_interval format (expected values like 5s, 5m, 1h): " + err.Error())
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
