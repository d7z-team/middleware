package kv

import (
	"context"
	"io"
	"time"
)

const TTLKeep = -1

type KV interface {
	List(ctx context.Context, prefix string) (map[string]string, error)
	ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error)
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) (bool, error)

	PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error)
	io.Closer
}
