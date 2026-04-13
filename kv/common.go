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

const maxListPairs = 10000

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
	// This is a small-dataset convenience API and may scan the full range in memory.
	List(ctx context.Context, prefix string) ([]Pair, error)
	// ListCurrent returns key-value pairs at the current level (excluding children).
	// This is a small-dataset convenience API and may scan the full range in memory.
	ListCurrent(ctx context.Context, prefix string) ([]Pair, error)
	// ListCursor returns a list of key-value pairs based on cursor and limit.
	// Cursor is an opaque backend-specific token and should be treated as such.
	// Redis uses SCAN-based pagination here, so item order is not guaranteed to be stable
	// across pages, and the cursor must not be interpreted as the last returned key.
	ListCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error)
	// ListCurrentCursor returns a list of key-value pairs at the current level based on cursor and limit.
	// Cursor is an opaque backend-specific token and should be treated as such.
	// Redis uses SCAN-based pagination here, so item order is not guaranteed to be stable
	// across pages, and the cursor must not be interpreted as the last returned key.
	ListCurrentCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error)
	// Scan performs a prefix scan with pagination support.
	// Cursor is an opaque backend-specific token and should be treated as such.
	// Redis uses SCAN-based pagination here, so item order is not guaranteed to be stable
	// across pages, and the cursor must not be interpreted as the last returned key.
	Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error)

	// PutBatch stores multiple key-value pairs with the same TTL.
	PutBatch(ctx context.Context, pairs []Pair, ttl time.Duration) error
	// GetBatch retrieves values for multiple keys.
	GetBatch(ctx context.Context, keys []string) ([]string, error)
	// DeleteBatch removes multiple keys.
	DeleteBatch(ctx context.Context, keys []string) error
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

type ListOptions struct {
	Limit int64
	// Cursor is an opaque token returned by a previous list call.
	// For Redis-backed implementations this is a SCAN continuation token, not a business key.
	Cursor string
}

type ListResponse struct {
	Pairs   []Pair
	Cursor  string
	HasMore bool
}

type ScanOptions struct {
	Prefix string
	// Cursor is an opaque token returned by a previous scan call.
	// For Redis-backed implementations this is a SCAN continuation token, not a business key.
	Cursor string
	Limit  int
}

type ScanResponse struct {
	Pairs      []Pair
	NextCursor string
	HasMore    bool
}

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

func isCurrentLevel(relKey, prefix string) bool {
	if !strings.HasPrefix(relKey, prefix) {
		return false
	}
	rest := relKey[len(prefix):]
	rest = strings.TrimPrefix(rest, "/")
	return rest != "" && !strings.Contains(rest, "/")
}

func normalizeKVPrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

// NewKVFromURL creates a new KV instance from a URL string.
// Supported schemes: memory, storage, local, etcd.
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
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

// Common error definitions.
var (
	ErrKeyNotFound  = errors.Join(os.ErrNotExist, errors.New("key not found"))
	ErrCASFailed    = errors.New("compare and swap failed")
	ErrInvalidKey   = errors.New("key must not contain '/'")
	ErrInvalidTTL   = errors.New("ttl must be TTLKeep or greater than 0")
	ErrListTooLarge = fmt.Errorf("list exceeds maximum size of %d", maxListPairs)
)

func invalidTTL(ttl time.Duration) bool {
	return ttl == 0 || ttl < TTLKeep
}

func normalizeListOptions(options *ListOptions) ListOptions {
	opts := ListOptions{Limit: 1000}
	if options != nil {
		opts = *options
		if opts.Limit <= 0 {
			opts.Limit = 1000
		}
	}
	return opts
}

func ensureListSize(count int64) error {
	if count > maxListPairs {
		return ErrListTooLarge
	}
	return nil
}
