package tools

import (
	"context"
	"encoding/json"
	"strings"

	"gopkg.d7z.net/middleware/kv"
)

type TypedConfig[Data comparable] struct {
	prefix string
	kv     kv.KV
}

func NewTypedConfig[Data comparable](kv kv.KV, prefix string) *TypedConfig[Data] {
	return &TypedConfig[Data]{
		kv:     kv,
		prefix: strings.ReplaceAll("typed::config::"+prefix+"::", "::", kv.Spliter()),
	}
}

func (c *TypedConfig[Data]) Load(key string) (Data, bool) {
	var zero Data
	fullKey := c.prefix + key
	valStr, err := c.kv.Get(context.Background(), fullKey)
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

	fullKey := c.prefix + key
	return c.kv.Put(context.Background(), fullKey, string(valBytes), kv.TTLKeep)
}

func (c *TypedConfig[Data]) Delete(key string) {
	fullKey := c.prefix + key
	_, _ = c.kv.Delete(context.Background(), fullKey)
}
