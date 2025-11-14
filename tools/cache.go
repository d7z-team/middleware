package tools

import (
	"context"
	"io"
	"strings"
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

type PrefixCache struct {
	prefix string
	cache  cache.Cache
}

func NewPrefixCache(prefix string, cache cache.Cache) *PrefixCache {
	return &PrefixCache{
		prefix: strings.TrimRight(prefix, "/") + "/",
		cache:  cache,
	}
}

func (p *PrefixCache) Put(ctx context.Context, key string, metadata map[string]string, value io.Reader, ttl time.Duration) error {
	return p.cache.Put(ctx, p.prefix+key, metadata, value, ttl)
}

func (p *PrefixCache) Get(ctx context.Context, key string) (*cache.Content, error) {
	return p.cache.Get(ctx, p.prefix+key)
}

func (p *PrefixCache) Delete(ctx context.Context, key string) error {
	return p.cache.Delete(ctx, p.prefix+key)
}

func (p *PrefixCache) Close() error {
	return p.cache.Close()
}
