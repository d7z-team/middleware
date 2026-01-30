package tools

import (
	"context"
	"encoding/json"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

type KVCache[Data any] struct {
	kv  kv.KV
	ttl time.Duration
}

func NewCache[Data any](kv kv.KV, prefix string, ttl time.Duration) *KVCache[Data] {
	return &KVCache[Data]{
		kv:  kv.Child(prefix),
		ttl: ttl,
	}
}

func (c *KVCache[Data]) Load(ctx context.Context, key string) (Data, bool) {
	var zero Data
	valStr, err := c.kv.Get(ctx, key)
	if err != nil {
		return zero, false
	}
	var data Data
	if err := json.Unmarshal([]byte(valStr), &data); err != nil {
		return zero, false
	}

	return data, true
}

func (c *KVCache[Data]) Store(ctx context.Context, key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.kv.Put(ctx, key, string(valBytes), c.ttl)
}

// Delete 从缓存中删除指定键
func (c *KVCache[Data]) Delete(ctx context.Context, key string) {
	_, _ = c.kv.Delete(ctx, key)
}
