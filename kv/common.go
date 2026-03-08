package kv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

// TTLKeep indicates that the TTL should not be modified.
const TTLKeep = -1

// Pair represents a key-value pair.
type Pair struct {
	Key   string
	Value string
}

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
	// DeleteAll removes all keys under the current prefix.
	DeleteAll(ctx context.Context) error
	// Count returns the number of keys.
	Count(ctx context.Context) (int64, error)
	// PutIfNotExists stores the key-value pair only if the key does not exist.
	PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// CompareAndSwap updates the value for a key only if the current value matches oldValue.
	CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error)

	// List returns all key-value pairs matching the prefix.
	List(ctx context.Context, prefix string) ([]Pair, error)
	// ListCurrent returns key-value pairs at the current level (excluding children).
	ListCurrent(ctx context.Context, prefix string) ([]Pair, error)
	// ListPage returns a page of key-value pairs matching the prefix.
	ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error)
	// ListCurrentPage returns a page of key-value pairs at the current level (excluding children).
	ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error)
	// ListCursor returns a list of key-value pairs based on cursor and limit.
	ListCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error)
	// ListCurrentCursor returns a list of key-value pairs at the current level based on cursor and limit.
	ListCurrentCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error)
	// Scan performs a prefix scan with pagination support.
	Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error)
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

// ListOptions defines options for list operations.
type ListOptions struct {
	Limit  int64  // Maximum number of items to return
	Cursor string // Cursor for pagination (last key of previous page)
}

// ListResponse contains the results of a list operation.
type ListResponse struct {
	Pairs   []Pair // Matched key-value pairs (relative to prefix, without root prefix)
	Cursor  string // Cursor for the next page (empty if no more data)
	HasMore bool   // Indicates if there is more data available
}

// ScanOptions defines parameters for the scan operation.
type ScanOptions struct {
	// Prefix is the search prefix relative to the current KV instance's root path.
	Prefix string
	// Cursor is the pagination cursor. Empty for the first page.
	Cursor string
	// Limit restricts the maximum number of pairs returned in a single call.
	Limit int
}

// ScanResponse defines the result of a scan operation.
type ScanResponse struct {
	// Pairs contains the matched key-value pairs.
	// Keys are relative to the current KV instance's root prefix.
	Pairs []Pair
	// NextCursor is the cursor for the next page. Empty if the scan is finished.
	NextCursor string
	// HasMore indicates if there is more data available to scan.
	HasMore bool
}

// listPageRange calculates the start and end indices for pagination.
func listPageRange(totalLen int, pageIndex uint64, pageSize uint) (int, int) {
	start := int(pageIndex * uint64(pageSize))
	if start >= totalLen {
		return totalLen, totalLen
	}
	end := start + int(pageSize)
	if end > totalLen {
		end = totalLen
	}
	return start, end
}

// listCursorStartIndex finds the starting index for cursor-based pagination.
func listCursorStartIndex(pairs []Pair, cursor string) int {
	if cursor == "" {
		return 0
	}
	for i, p := range pairs {
		if p.Key > cursor {
			return i
		}
	}
	return len(pairs)
}

// isCurrentLevel checks if the relative key is at the current level for the given prefix.
func isCurrentLevel(relKey, prefix string) bool {
	if !strings.HasPrefix(relKey, prefix) {
		return false
	}
	rest := relKey[len(prefix):]
	rest = strings.TrimPrefix(rest, "/")
	return rest != "" && !strings.Contains(rest, "/")
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
	ErrInvalidKey  = errors.New("key must not contain '/'")
)
