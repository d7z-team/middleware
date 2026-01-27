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

// TTLKeep indicates that the TTL should not be modified.
const TTLKeep = -1

// KV defines the basic key-value storage interface.
type KV interface {
	// Child creates a child KV with the given path segments appended to the current prefix.
	Child(paths ...string) KV
	// Put stores a key-value pair with an optional TTL.
	Put(ctx context.Context, key, value string, ttl time.Duration) error
	// Get retrieves the value associated with the key.
	Get(ctx context.Context, key string) (string, error)
	// Delete removes the value associated with the key.
	Delete(ctx context.Context, key string) (bool, error)
	// Count returns the number of keys.
	Count(ctx context.Context) (int64, error)
	// PutIfNotExists stores the key-value pair only if the key does not exist.
	PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// CompareAndSwap updates the value for a key only if the current value matches oldValue.
	CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error)
}

// RawKV interface allows access to the underlying KV implementation.
type RawKV interface {
	Raw() KV
}

// CloserKV combines the KV interface with io.Closer.
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

func (receiver closerKV) Raw() KV {
	return receiver.KV
}

// PagedKV extends KV with pagination support.
type PagedKV interface {
	KV
	// List returns all key-value pairs matching the prefix.
	List(ctx context.Context, prefix string) (map[string]string, error)
	// ListPage returns a page of key-value pairs matching the prefix.
	ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error)
}

// ListOptions defines options for list operations.
type ListOptions struct {
	Limit  int64  // Maximum number of items to return
	Cursor string // Cursor for pagination (last key of previous page)
}

// ListResponse contains the results of a list operation.
type ListResponse struct {
	Keys    []string // Matched keys (relative to prefix, without root prefix)
	Cursor  string   // Cursor for the next page (empty if no more data)
	HasMore bool     // Indicates if there is more data available
}

// CursorPagedKV extends KV with cursor-based pagination support.
type CursorPagedKV interface {
	KV
	// CursorList returns a list of keys based on cursor and limit.
	CursorList(ctx context.Context, opts *ListOptions) (*ListResponse, error)
}

// NewKVFromURL creates a new KV instance from a URL string.
// Supported schemes: memory, storage, local, etcd, redis.
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

// Common error definitions.
var (
	ErrKeyNotFound = errors.Join(os.ErrNotExist, errors.New("key not found"))
	ErrCASFailed   = errors.New("compare and swap failed")
)
