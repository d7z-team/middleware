package tools

import (
	"context"
	"encoding/json"

	"gopkg.d7z.net/middleware/kv"
)

type TypedConfig[Data comparable] struct {
	kv kv.KV
}

func NewTypedConfig[Data comparable](kv kv.KV, prefix string) *TypedConfig[Data] {
	return &TypedConfig[Data]{
		kv: kv.Child(prefix),
	}
}

func (c *TypedConfig[Data]) Load(ctx context.Context, key string) (Data, bool) {
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

func (c *TypedConfig[Data]) Store(ctx context.Context, key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.kv.Put(ctx, key, string(valBytes), kv.TTLKeep)
}

func (c *TypedConfig[Data]) Delete(ctx context.Context, key string) {
	_, _ = c.kv.Delete(ctx, key)
}
