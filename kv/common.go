package kv

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

const TTLKeep = -1

type KV interface {
	Spliter() string

	List(ctx context.Context, prefix string) (map[string]string, error)
	ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error)
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) (bool, error)

	PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error)
	io.Closer
}

func NewKVFromURL(s string) (KV, error) {
	parse, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch parse.Scheme {
	case "memory", "mem":
		return NewMemory("")
	case "storage", "local":
		return NewMemory(parse.Path)
	case "etcd":
		etcd, err := connects.NewEtcd(parse)
		if err != nil {
			return nil, err
		}
		return NewEtcd(etcd, parse.Query().Get("prefix")), nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}
