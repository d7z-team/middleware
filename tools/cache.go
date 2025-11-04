package tools

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	kv "gopkg.d7z.net/middleware/kv"
)

type Cache[Data any] struct {
	prefix string
	kv     kv.KV
	ttl    time.Duration
}

func NewCache[Data any](kv kv.KV, prefix string, ttl time.Duration) *Cache[Data] {
	return &Cache[Data]{
		kv:     kv,
		prefix: strings.ReplaceAll("cache::"+prefix+"::", "::", kv.Spliter()),
		ttl:    ttl,
	}
}

func (c *Cache[Data]) Load(key string) (Data, bool) {
	var zero Data
	fullKey := c.prefix + key
	valStr, err := c.kv.Get(context.Background(), fullKey)
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

func (c *Cache[Data]) Store(key string, value Data) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	fullKey := c.prefix + key
	return c.kv.Put(context.Background(), fullKey, string(valBytes), c.ttl)
}

// Delete 从缓存中删除指定键
func (c *Cache[Data]) Delete(key string) {
	fullKey := c.prefix + key
	_, _ = c.kv.Delete(context.Background(), fullKey)
}
