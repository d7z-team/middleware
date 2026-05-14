package kv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const badgerMaxRetries = 8

// Badger is a badger-based KV storage implementation.
type Badger struct {
	db     *badger.DB
	prefix string
}

// NewBadger opens a badger DB and returns a KV view rooted at prefix.
func NewBadger(path, prefix string) (*Badger, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return NewBadgerFromDB(db, prefix), nil
}

// NewBadgerFromDB wraps an existing badger DB with the given prefix.
func NewBadgerFromDB(db *badger.DB, prefix string) *Badger {
	return &Badger{
		db:     db,
		prefix: normalizeKVPrefix(prefix),
	}
}

func (b *Badger) Child(paths ...string) KV {
	newPrefix := childKVPrefix(b.prefix, paths...)
	if newPrefix == b.prefix {
		return b
	}
	return NewBadgerFromDB(b.db, newPrefix)
}

func (b *Badger) buildKey(key string) ([]byte, error) {
	key = strings.Trim(key, "/")
	if key == "" || strings.Contains(key, "/") {
		return nil, ErrInvalidKey
	}
	return []byte(b.prefix + key), nil
}

func (b *Badger) prefixed(prefix string) []byte {
	return []byte(b.prefix + strings.TrimLeft(prefix, "/"))
}

func (b *Badger) extractKey(fullKey []byte) string {
	return strings.TrimPrefix(string(fullKey), b.prefix)
}

func (b *Badger) withRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < badgerMaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		err = fn()
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	return err
}

func (b *Badger) ttlFromItem(item *badger.Item) time.Duration {
	expiresAt := item.ExpiresAt()
	if expiresAt == 0 {
		return TTLKeep
	}
	remaining := time.Until(time.Unix(int64(expiresAt), 0))
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (b *Badger) setWithTTL(txn *badger.Txn, key []byte, value string, ttl time.Duration) error {
	entry := badger.NewEntry(key, []byte(value))
	if ttl > 0 {
		entry = entry.WithTTL(ttl)
	}
	return txn.SetEntry(entry)
}

func (b *Badger) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	fullKey, err := b.buildKey(key)
	if err != nil {
		return err
	}

	return b.withRetry(ctx, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			if ttl != TTLKeep {
				return b.setWithTTL(txn, fullKey, value, ttl)
			}

			item, err := txn.Get(fullKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return txn.Set(fullKey, []byte(value))
				}
				return err
			}

			remaining := b.ttlFromItem(item)
			if remaining > 0 {
				return b.setWithTTL(txn, fullKey, value, remaining)
			}
			return txn.Set(fullKey, []byte(value))
		})
	})
}

func (b *Badger) Get(ctx context.Context, key string) (string, error) {
	fullKey, err := b.buildKey(key)
	if err != nil {
		return "", err
	}

	var value string
	err = b.db.View(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		item, err := txn.Get(fullKey)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrKeyNotFound
			}
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(val)
		return nil
	})
	return value, err
}

func (b *Badger) Delete(ctx context.Context, key string) (bool, error) {
	fullKey, err := b.buildKey(key)
	if err != nil {
		return false, err
	}

	var deleted bool
	err = b.withRetry(ctx, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			_, err := txn.Get(fullKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					deleted = false
					return nil
				}
				return err
			}
			deleted = true
			return txn.Delete(fullKey)
		})
	})
	return deleted, err
}

func (b *Badger) DeleteAll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if b.prefix == "" {
		return b.db.DropAll()
	}
	return b.db.DropPrefix([][]byte{[]byte(b.prefix)}...)
}

func (b *Badger) Count(ctx context.Context) (int64, error) {
	var count int64
	prefix := []byte(b.prefix)
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			count++
		}
		return nil
	})
	return count, err
}

func (b *Badger) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if invalidTTL(ttl) {
		return false, ErrInvalidTTL
	}
	fullKey, err := b.buildKey(key)
	if err != nil {
		return false, err
	}

	var ok bool
	err = b.withRetry(ctx, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(fullKey)
			if err == nil {
				if item.IsDeletedOrExpired() {
					ok = true
					return b.setWithTTL(txn, fullKey, value, ttl)
				}
				ok = false
				return nil
			}
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			ok = true
			return b.setWithTTL(txn, fullKey, value, ttl)
		})
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (b *Badger) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	fullKey, err := b.buildKey(key)
	if err != nil {
		return false, err
	}

	var swapped bool
	err = b.withRetry(ctx, func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(fullKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					swapped = false
					return nil
				}
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if string(val) != oldValue {
				swapped = false
				return nil
			}
			remaining := b.ttlFromItem(item)
			swapped = true
			if remaining > 0 {
				return b.setWithTTL(txn, fullKey, newValue, remaining)
			}
			return txn.Set(fullKey, []byte(newValue))
		})
	})
	if err != nil {
		return false, err
	}
	return swapped, nil
}

func (b *Badger) List(ctx context.Context, prefix string) ([]Pair, error) {
	pairs := make([]Pair, 0)
	fullPrefix := b.prefixed(prefix)
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = fullPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(fullPrefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			pairs = append(pairs, Pair{
				Key:   b.extractKey(item.KeyCopy(nil)),
				Value: string(val),
			})
			if err := ensureListSize(int64(len(pairs))); err != nil {
				return err
			}
		}
		return nil
	})
	return pairs, err
}

func (b *Badger) ListCurrent(ctx context.Context, prefix string) ([]Pair, error) {
	pairs := make([]Pair, 0)
	fullPrefix := b.prefixed(prefix)
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = fullPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(fullPrefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			item := it.Item()
			relKey := b.extractKey(item.KeyCopy(nil))
			if !isCurrentLevel(relKey, prefix) {
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			pairs = append(pairs, Pair{
				Key:   relKey,
				Value: string(val),
			})
			if err := ensureListSize(int64(len(pairs))); err != nil {
				return err
			}
		}
		return nil
	})
	return pairs, err
}

func (b *Badger) ListCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	normalized := normalizeListOptions(opts)
	res, err := b.Scan(ctx, ScanOptions{Cursor: normalized.Cursor, Limit: int(normalized.Limit)})
	if err != nil {
		return nil, err
	}
	return &ListResponse{Pairs: res.Pairs, Cursor: res.NextCursor, HasMore: res.HasMore}, nil
}

func (b *Badger) ListCurrentCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	all, err := b.ListCurrent(ctx, "")
	if err != nil {
		return nil, err
	}
	normalized := normalizeListOptions(opts)
	start := listCursorStartIndex(all, normalized.Cursor)
	limit := int(normalized.Limit)
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	cursor := ""
	if end < len(all) {
		cursor = all[end-1].Key
	}
	return &ListResponse{
		Pairs:   all[start:end],
		Cursor:  cursor,
		HasMore: end < len(all),
	}, nil
}

func (b *Badger) Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}

	res := &ScanResponse{Pairs: make([]Pair, 0, opts.Limit)}
	fullPrefix := b.prefixed(opts.Prefix)
	start := fullPrefix
	if opts.Cursor != "" {
		start = []byte(b.prefix + opts.Cursor + "\x00")
	}

	err := b.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.Prefix = fullPrefix
		it := txn.NewIterator(iterOpts)
		defer it.Close()

		it.Seek(start)
		for ; it.ValidForPrefix(fullPrefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(res.Pairs) >= opts.Limit {
				res.HasMore = true
				break
			}
			res.Pairs = append(res.Pairs, Pair{
				Key:   b.extractKey(item.KeyCopy(nil)),
				Value: string(val),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if res.HasMore && len(res.Pairs) > 0 {
		res.NextCursor = res.Pairs[len(res.Pairs)-1].Key
	}
	return res, nil
}

func (b *Badger) PutBatch(ctx context.Context, pairs []Pair, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	if len(pairs) == 0 {
		return nil
	}
	if ttl == TTLKeep {
		for _, p := range pairs {
			if err := b.Put(ctx, p.Key, p.Value, ttl); err != nil {
				return err
			}
		}
		return nil
	}

	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for _, p := range pairs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		fullKey, err := b.buildKey(p.Key)
		if err != nil {
			return err
		}
		entry := badger.NewEntry(fullKey, []byte(p.Value))
		if ttl > 0 {
			entry = entry.WithTTL(ttl)
		}
		if err := wb.SetEntry(entry); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (b *Badger) GetBatch(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}
	values := make([]string, len(keys))
	err := b.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			fullKey, err := b.buildKey(key)
			if err != nil {
				return err
			}
			item, err := txn.Get(fullKey)
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
				}
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			values[i] = string(val)
		}
		return nil
	})
	return values, err
}

func (b *Badger) DeleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		fullKey, err := b.buildKey(key)
		if err != nil {
			return err
		}
		if err := wb.Delete(fullKey); err != nil {
			return err
		}
	}
	return wb.Flush()
}
