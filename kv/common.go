package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

const TTLKeep = -1

type KV interface {
	Child(path string) KV
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Delete(ctx context.Context, key string) (bool, error)

	PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error)
}

type CloserKV interface {
	KV
	io.Closer
}

type closerKV struct {
	KV
	closer func() error
}

func (receiver closerKV) Close() error {
	return receiver.closer()
}

type PagedKV interface {
	KV
	List(ctx context.Context, prefix string) (map[string]string, error)
	ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error)
}

type ListOptions struct {
	Limit  int64  // 最大返回数量
	Cursor string // 分页游标（上一页最后一个键）
}

type ListResponse struct {
	Keys    []string // 匹配的键（相对前缀，不含根前缀）
	Cursor  string   // 下一页游标（为空表示没有更多数据）
	HasMore bool     // 是否还有更多数据
}

type CursorPagedKV interface {
	KV
	CursorList(ctx context.Context, opts *ListOptions) (*ListResponse, error)
}

func NewKVFromURL(s string) (CloserKV, error) {
	parse, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch parse.Scheme {
	case "memory", "mem":
		memory, err := NewMemory("")
		if err != nil {
			return nil, err
		}
		return closerKV{
			KV:     memory,
			closer: memory.Sync,
		}, nil
	case "storage", "local":
		memory, err := NewMemory(parse.Path)
		if err != nil {
			return nil, err
		}
		return closerKV{
			KV:     memory,
			closer: memory.Sync,
		}, nil
	case "etcd":
		etcd, err := connects.NewEtcd(parse)
		if err != nil {
			return nil, err
		}
		return closerKV{
			KV:     NewEtcd(etcd, parse.Query().Get("prefix")),
			closer: etcd.Close,
		}, nil
	case "redis":
		redis, err := connects.NewRedis(parse)
		if err != nil {
			return nil, err
		}
		return closerKV{
			KV:     NewRedis(redis, parse.Query().Get("prefix")),
			closer: redis.Close,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

// 确保 KV 包中存在以下错误定义（已补充到 kv.go）
var (
	ErrKeyNotFound = errors.Join(os.ErrNotExist, errors.New("key not found"))
	ErrCASFailed   = errors.New("compare and swap failed")
)
