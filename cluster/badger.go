package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type badgerStore struct {
	db        *badger.DB
	prefix    string
	retention int
	hub       *watchHub
	mu        sync.RWMutex
	closed    bool
}

func newBadgerStore(path string, options Options) (*badgerStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrInvalidConfig
	}
	opts := badger.DefaultOptions(filepath.Clean(path))
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerStore{
		db:        db,
		prefix:    normalizeStorePrefix(options.Prefix),
		retention: options.EventRetentionCount,
		hub:       newWatchHub(),
	}, nil
}

func (s *badgerStore) get(ctx context.Context, ref objectRef) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	var out Unstructured
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(s.objectKey(ref)))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			return err
		}
		return item.Value(func(value []byte) error {
			return json.Unmarshal(value, &out)
		})
	})
	if err != nil {
		return nil, err
	}
	return cloneUnstructuredPtr(&out), nil
}

func (s *badgerStore) list(ctx context.Context, resource string) ([]Unstructured, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	objects := make([]Unstructured, 0)
	var rv uint64
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		rv, err = s.currentRV(txn)
		if err != nil {
			return err
		}
		prefix := s.objectPrefix(resource)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix([]byte(prefix)); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var obj Unstructured
			if err := it.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &obj)
			}); err != nil {
				return err
			}
			objects = append(objects, cloneUnstructured(obj))
		}
		return nil
	})
	return objects, rv, err
}

func (s *badgerStore) commit(ctx context.Context, req commitRequest) (*Unstructured, resourceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, resourceEvent{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, resourceEvent{}, ErrClosed
	}

	var out Unstructured
	var event resourceEvent
	err := s.db.Update(func(txn *badger.Txn) error {
		current, exists, err := s.getObjectTxn(txn, req.Ref)
		if err != nil {
			return err
		}
		switch req.Op {
		case commitCreate:
			if exists {
				return ErrAlreadyExists
			}
		case commitUpdate, commitDelete:
			if !exists {
				return ErrNotFound
			}
			if parseStoredRV(current.Metadata.ResourceVersion) != req.ExpectedRV {
				return ErrConflict
			}
		default:
			return ErrUnsupported
		}

		currentRV, err := s.currentRV(txn)
		if err != nil {
			return err
		}
		nextRV := currentRV + 1
		out = cloneUnstructured(*req.Object)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		raw, err := json.Marshal(out)
		if err != nil {
			return err
		}
		if req.Op == commitDelete {
			if err := txn.Delete([]byte(s.objectKey(req.Ref))); err != nil {
				return err
			}
		} else if err := txn.Set([]byte(s.objectKey(req.Ref)), raw); err != nil {
			return err
		}

		event = newStoreEvent(req, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(s.eventAllKey(nextRV)), eventRaw); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.eventResourceKey(req.Ref.Resource, nextRV)), eventRaw); err != nil {
			return err
		}
		return s.setRV(txn, nextRV)
	})
	if err != nil {
		return nil, resourceEvent{}, err
	}
	s.hub.notify(req.Ref.Resource)
	return cloneUnstructuredPtr(&out), event, nil
}

func (s *badgerStore) eventsAfter(ctx context.Context, after uint64, resource string, limit int) ([]resourceEvent, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	events := make([]resourceEvent, 0, limit)
	var rv uint64
	err := s.db.View(func(txn *badger.Txn) error {
		compacted, err := s.compactedRV(txn)
		if err != nil {
			return err
		}
		rv, err = s.currentRV(txn)
		if err != nil {
			return err
		}
		if after < compacted {
			return ErrResourceVersionTooOld
		}
		prefix := s.eventPrefix(resource)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(prefix + rvKey(after+1))); it.ValidForPrefix([]byte(prefix)); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var event resourceEvent
			if err := it.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &event)
			}); err != nil {
				return err
			}
			events = append(events, cloneEvent(event))
			if len(events) >= limit {
				break
			}
		}
		return nil
	})
	return events, rv, err
}

func (s *badgerStore) cleanupEvents(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := s.cleanupEventsCutoff(ctx)
	if err != nil || before == 0 {
		return err
	}
	for {
		deleted, err := s.deleteEventsBeforeBatch(ctx, before, defaultEventBatchSize)
		if err != nil || deleted < defaultEventBatchSize {
			return err
		}
	}
}

func (s *badgerStore) subscribe(ctx context.Context, resource string) (<-chan struct{}, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.hub.subscribe(resource)
}

func (s *badgerStore) acquireNode(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token, err := randomToken("node")
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	record := nodeLeaseRecord{Token: token, ExpiresAt: now.Add(ttl)}
	raw, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	err = s.db.Update(func(txn *badger.Txn) error {
		current, exists, err := s.getNodeLeaseTxn(txn, name)
		if err != nil {
			return err
		}
		if exists && current.ExpiresAt.After(now) {
			return ErrNodeAlreadyExists
		}
		return txn.Set([]byte(s.nodeLeaseKey(name)), raw)
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

func (s *badgerStore) renewNode(ctx context.Context, name, token string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	now := time.Now().UTC()
	return s.db.Update(func(txn *badger.Txn) error {
		record, exists, err := s.getNodeLeaseTxn(txn, name)
		if err != nil {
			return err
		}
		if !exists || record.Token != token || !record.ExpiresAt.After(now) {
			return ErrNodeLeaseLost
		}
		record.ExpiresAt = now.Add(ttl)
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		return txn.Set([]byte(s.nodeLeaseKey(name)), raw)
	})
}

func (s *badgerStore) releaseNode(ctx context.Context, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		record, exists, err := s.getNodeLeaseTxn(txn, name)
		if err != nil || !exists || record.Token != token {
			return err
		}
		err = txn.Delete([]byte(s.nodeLeaseKey(name)))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
}

func (s *badgerStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.hub.close()
	return s.db.Close()
}

func (s *badgerStore) getObjectTxn(txn *badger.Txn, ref objectRef) (Unstructured, bool, error) {
	item, err := txn.Get([]byte(s.objectKey(ref)))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return Unstructured{}, false, nil
		}
		return Unstructured{}, false, err
	}
	var obj Unstructured
	if err := item.Value(func(value []byte) error {
		return json.Unmarshal(value, &obj)
	}); err != nil {
		return Unstructured{}, false, err
	}
	return obj, true, nil
}

func (s *badgerStore) getNodeLeaseTxn(txn *badger.Txn, name string) (nodeLeaseRecord, bool, error) {
	item, err := txn.Get([]byte(s.nodeLeaseKey(name)))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nodeLeaseRecord{}, false, nil
		}
		return nodeLeaseRecord{}, false, err
	}
	var record nodeLeaseRecord
	if err := item.Value(func(value []byte) error {
		return json.Unmarshal(value, &record)
	}); err != nil {
		return nodeLeaseRecord{}, false, err
	}
	return record, true, nil
}

func (s *badgerStore) currentRV(txn *badger.Txn) (uint64, error) {
	return s.readUint(txn, s.metaKey("rv"))
}

func (s *badgerStore) compactedRV(txn *badger.Txn) (uint64, error) {
	return s.readUint(txn, s.metaKey("compacted-rv"))
}

func (s *badgerStore) readUint(txn *badger.Txn, key string) (uint64, error) {
	item, err := txn.Get([]byte(key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	var value uint64
	err = item.Value(func(raw []byte) error {
		value = parseStoredRV(string(raw))
		return nil
	})
	return value, err
}

func (s *badgerStore) setRV(txn *badger.Txn, rv uint64) error {
	return txn.Set([]byte(s.metaKey("rv")), []byte(formatRV(rv)))
}

func (s *badgerStore) setCompactedRV(txn *badger.Txn, rv uint64) error {
	return txn.Set([]byte(s.metaKey("compacted-rv")), []byte(formatRV(rv)))
}

func (s *badgerStore) cleanupEventsCutoff(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrClosed
	}
	var before uint64
	err := s.db.View(func(txn *badger.Txn) error {
		current, err := s.currentRV(txn)
		if err != nil {
			return err
		}
		if current > uint64(s.retention) {
			before = current - uint64(s.retention)
		}
		return nil
	})
	return before, err
}

func (s *badgerStore) deleteEventsBeforeBatch(ctx context.Context, before uint64, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	var deleted int
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		deleted, err = s.deleteEventsBeforeTxn(ctx, txn, before, limit)
		return err
	})
	return deleted, err
}

func (s *badgerStore) deleteEventsBeforeTxn(ctx context.Context, txn *badger.Txn, before uint64, limit int) (int, error) {
	previous, err := s.compactedRV(txn)
	if err != nil {
		return 0, err
	}
	if before > previous {
		if err := s.setCompactedRV(txn, before); err != nil {
			return 0, err
		}
	}

	prefix := s.eventPrefix("")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)
	it := txn.NewIterator(opts)
	defer it.Close()
	toDelete := make([]resourceEvent, 0, limit)
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if parseRVKey(string(it.Item().Key())) > before {
			break
		}
		var event resourceEvent
		if err := it.Item().Value(func(value []byte) error {
			return json.Unmarshal(value, &event)
		}); err != nil {
			return 0, err
		}
		toDelete = append(toDelete, event)
		if len(toDelete) >= limit {
			break
		}
	}
	for _, event := range toDelete {
		rv := parseStoredRV(event.ResourceVersion)
		if err := txn.Delete([]byte(s.eventAllKey(rv))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return 0, err
		}
		if err := txn.Delete([]byte(s.eventResourceKey(event.Ref.Resource, rv))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return 0, err
		}
	}
	return len(toDelete), nil
}

func (s *badgerStore) metaKey(name string) string {
	return s.prefix + "meta/" + name
}

func (s *badgerStore) objectKey(ref objectRef) string {
	return s.prefix + "objects/" + ref.Resource + "/" + ref.Name
}

func (s *badgerStore) objectPrefix(resource string) string {
	if resource == "" {
		return s.prefix + "objects/"
	}
	return s.prefix + "objects/" + resource + "/"
}

func (s *badgerStore) eventPrefix(resource string) string {
	if resource == "" {
		return s.prefix + "events/all/"
	}
	return s.prefix + "events/resources/" + resource + "/"
}

func (s *badgerStore) eventAllKey(rv uint64) string {
	return s.eventPrefix("") + rvKey(rv)
}

func (s *badgerStore) eventResourceKey(resource string, rv uint64) string {
	return s.eventPrefix(resource) + rvKey(rv)
}

func (s *badgerStore) nodeLeaseKey(name string) string {
	return s.prefix + "leases/nodes/" + name
}
