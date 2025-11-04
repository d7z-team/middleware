package l2cache

import (
	"errors"
	"io"
	"os"
	"time"
)

const TTLKeep = -1

type Cache interface {
	Put(key string, value io.Reader, ttl time.Duration) error
	Get(key string) (*CacheContent, error)
	Delete(key string) error
	io.Closer
}

type CacheContent struct {
	io.ReadSeekCloser
	Length       int
	LastModified time.Time
}

func (c *CacheContent) ReadToString() (string, error) {
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
