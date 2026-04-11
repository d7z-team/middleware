package tools

import (
	"context"
	"encoding/json"
	"errors"

	"gopkg.d7z.net/middleware/kv"
)

type TypedConfig[Data any] struct {
	kv kv.KV
}

func NewTypedConfig[Data any](kv kv.KV, prefix string) *TypedConfig[Data] {
	return &TypedConfig[Data]{
		kv: kv.Child(prefix),
	}
}

func (c *TypedConfig[Data]) Load(ctx context.Context, key string) (Data, bool, error) {
	var zero Data
	valStr, err := c.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return zero, false, nil
		}
		return zero, false, err
	}
	var data Data
	if err := json.Unmarshal([]byte(valStr), &data); err != nil {
		return zero, false, err
	}

	return data, true, nil
}

func (c *TypedConfig[Data]) Store(ctx context.Context, key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.kv.Put(ctx, key, string(valBytes), kv.TTLKeep)
}

func (c *TypedConfig[Data]) Delete(ctx context.Context, key string) error {
	_, err := c.kv.Delete(ctx, key)
	return err
}
