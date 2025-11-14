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

func (c *TypedConfig[Data]) Load(key string) (Data, bool) {
	var zero Data
	valStr, err := c.kv.Get(context.Background(), key)
	if err != nil {
		return zero, false
	}
	var data Data
	if err := json.Unmarshal([]byte(valStr), &data); err != nil {
		return zero, false
	}

	return data, true
}

func (c *TypedConfig[Data]) Store(key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return c.kv.Put(context.Background(), key, string(valBytes), kv.TTLKeep)
}

func (c *TypedConfig[Data]) Delete(key string) {
	_, _ = c.kv.Delete(context.Background(), key)
}
