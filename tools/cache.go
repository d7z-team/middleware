package tools

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

type Cache[Data any] struct {
	prefix string
	kv     kv.KV
	ttl    time.Duration
}

func NewCache[Data any](kv kv.KV, prefix string, ttl time.Duration) *Cache[Data] {
	prefix = strings.ReplaceAll(prefix, kv.Splitter(), "")
	return &Cache[Data]{
		kv:     kv,
		prefix: kv.WithKey("typed", "cache", prefix),
		ttl:    ttl,
	}
}

func (c *Cache[Data]) Load(ctx context.Context, key string) (Data, bool) {
	var zero Data
	fullKey := c.kv.WithKey(c.prefix, key)
	valStr, err := c.kv.Get(ctx, fullKey)
	if err != nil {
		return zero, false
	}
	var data Data
	if reflect.TypeOf(data).Kind() == reflect.Ptr {
		if err := json.Unmarshal([]byte(valStr), data); err != nil {
			return zero, false
		}
	} else {
		if err := json.Unmarshal([]byte(valStr), &data); err != nil {
			return zero, false
		}
	}

	return data, true
}

func (c *Cache[Data]) Store(ctx context.Context, key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	fullKey := c.kv.WithKey(c.prefix, key)
	return c.kv.Put(ctx, fullKey, string(valBytes), c.ttl)
}

// Delete 从缓存中删除指定键
func (c *Cache[Data]) Delete(ctx context.Context, key string) {
	fullKey := c.kv.WithKey(c.prefix, key)
	_, _ = c.kv.Delete(ctx, fullKey)
}
