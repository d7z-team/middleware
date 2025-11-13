package tools

import (
	"context"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

type PrefixKV struct {
	prefix string
	kv     kv.KV
}

func NewPrefixKV(kv kv.KV, prefix string) *PrefixKV {
	prefix = strings.TrimPrefix(prefix, kv.Splitter())
	prefix += kv.Splitter()
	return &PrefixKV{
		prefix: prefix,
		kv:     kv,
	}
}

func (p *PrefixKV) Splitter() string {
	return p.kv.Splitter()
}

func (p *PrefixKV) WithKey(keys ...string) string {
	data := []string{p.prefix}
	return p.kv.WithKey(append(data, keys...)...)
}

func (p *PrefixKV) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	key = p.prefix + strings.TrimPrefix(key, p.kv.Splitter())
	return p.kv.Put(ctx, key, value, ttl)
}

func (p *PrefixKV) Get(ctx context.Context, key string) (string, error) {
	key = p.prefix + strings.TrimPrefix(key, p.kv.Splitter())
	return p.kv.Get(ctx, key)
}

func (p *PrefixKV) Delete(ctx context.Context, key string) (bool, error) {
	key = p.prefix + strings.TrimPrefix(key, p.kv.Splitter())
	return p.kv.Delete(ctx, key)
}

func (p *PrefixKV) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	key = p.prefix + strings.TrimPrefix(key, p.kv.Splitter())
	return p.kv.PutIfNotExists(ctx, key, value, ttl)
}

func (p *PrefixKV) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	key = p.prefix + strings.TrimPrefix(key, p.kv.Splitter())
	return p.kv.CompareAndSwap(ctx, key, oldValue, newValue)
}

func (p *PrefixKV) Close() error {
	return p.kv.Close()
}
