package tools

import (
	"context"
	"io"
	"time"

	"gopkg.d7z.net/middleware/cache"
)

type TTLCache struct {
	cache cache.Cache
	ttl   time.Duration
}

func NewTTLCache(cache cache.Cache, ttl time.Duration) *TTLCache {
	return &TTLCache{
		cache: cache,
		ttl:   ttl,
	}
}

func (c *TTLCache) Put(ctx context.Context, key string, metadata map[string]string, value io.Reader) error {
	return c.cache.Put(ctx, key, metadata, value, c.ttl)
}

func (c *TTLCache) Get(ctx context.Context, key string) (*cache.Content, error) {
	return c.cache.Get(ctx, key)
}

func (c *TTLCache) Delete(ctx context.Context, key string) error {
	return c.cache.Delete(ctx, key)
}
