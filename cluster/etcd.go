package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const etcdCommitRetries = 16

type etcdStore struct {
	client    *clientv3.Client
	prefix    string
	retention int
	mu        sync.RWMutex
	closed    bool
}

func newEtcdStore(client *clientv3.Client, options Options) *etcdStore {
	return &etcdStore{
		client:    client,
		prefix:    normalizeStorePrefix(options.Prefix),
		retention: options.EventRetentionCount,
	}
}

func (s *etcdStore) get(ctx context.Context, ref objectRef) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	resp, err := s.client.Get(ctx, s.objectKey(ref), clientv3.WithLimit(1))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}
	var obj Unstructured
	if err := json.Unmarshal(resp.Kvs[0].Value, &obj); err != nil {
		return nil, err
	}
	return cloneUnstructuredPtr(&obj), nil
}

func (s *etcdStore) list(ctx context.Context, resource string) ([]Unstructured, uint64, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, 0, err
	}
	prefix := s.objectPrefix(resource)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, 0, err
	}
	objects := make([]Unstructured, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var obj Unstructured
		if err := json.Unmarshal(kv.Value, &obj); err != nil {
			return nil, 0, err
		}
		objects = append(objects, cloneUnstructured(obj))
	}
	rv, err := s.currentRV(ctx)
	if err != nil {
		return nil, 0, err
	}
	return objects, rv, nil
}

func (s *etcdStore) commit(ctx context.Context, req commitRequest) (*Unstructured, resourceEvent, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, resourceEvent{}, err
	}
	var lastErr error
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, resourceEvent{}, err
		}
		objResp, err := s.client.Get(ctx, s.objectKey(req.Ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, resourceEvent{}, err
		}
		exists := len(objResp.Kvs) > 0
		var current Unstructured
		var currentRaw []byte
		if exists {
			currentRaw = append([]byte(nil), objResp.Kvs[0].Value...)
			if err := json.Unmarshal(currentRaw, &current); err != nil {
				return nil, resourceEvent{}, err
			}
		}
		switch req.Op {
		case commitCreate:
			if exists {
				return nil, resourceEvent{}, ErrAlreadyExists
			}
		case commitUpdate, commitDelete:
			if !exists {
				return nil, resourceEvent{}, ErrNotFound
			}
			if parseStoredRV(current.Metadata.ResourceVersion) != req.ExpectedRV {
				return nil, resourceEvent{}, ErrConflict
			}
		default:
			return nil, resourceEvent{}, ErrUnsupported
		}

		nextRV := currentRV + 1
		out := cloneUnstructured(*req.Object)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, resourceEvent{}, err
		}
		event := newStoreEvent(req, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, resourceEvent{}, err
		}

		cmps := []clientv3.Cmp{s.metaRVCompare(metaRaw, metaVersion)}
		if req.Op == commitCreate {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(req.Ref)), "=", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(req.Ref)), "=", string(currentRaw)))
		}
		ops := []clientv3.Op{
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.eventAllKey(nextRV), string(eventRaw)),
			clientv3.OpPut(s.eventResourceKey(req.Ref.Resource, nextRV), string(eventRaw)),
			clientv3.OpPut(s.notifyKey(""), formatRV(nextRV)),
			clientv3.OpPut(s.notifyKey(req.Ref.Resource), formatRV(nextRV)),
		}
		if req.Op == commitDelete {
			ops = append(ops, clientv3.OpDelete(s.objectKey(req.Ref)))
		} else {
			ops = append(ops, clientv3.OpPut(s.objectKey(req.Ref), string(objectRaw)))
		}

		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, resourceEvent{}, err
		}
		if txnResp.Succeeded {
			if s.retention > 0 {
				_ = s.enforceRetention(ctx, s.retention)
			}
			return cloneUnstructuredPtr(&out), event, nil
		}
		lastErr = ErrConflict
		select {
		case <-ctx.Done():
			return nil, resourceEvent{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return nil, resourceEvent{}, lastErr
	}
	return nil, resourceEvent{}, ErrConflict
}

func (s *etcdStore) eventsAfter(ctx context.Context, after uint64, resource string, limit int) ([]resourceEvent, uint64, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, 0, err
	}
	compacted, err := s.readUint(ctx, s.metaKey("compacted-rv"))
	if err != nil {
		return nil, 0, err
	}
	current, err := s.currentRV(ctx)
	if err != nil {
		return nil, 0, err
	}
	if after < compacted {
		return nil, current, ErrResourceVersionTooOld
	}
	prefix := s.eventPrefix(resource)
	resp, err := s.client.Get(
		ctx,
		prefix+rvKey(after+1),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		clientv3.WithLimit(int64(limit)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, 0, err
	}
	events := make([]resourceEvent, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var event resourceEvent
		if err := json.Unmarshal(kv.Value, &event); err != nil {
			return nil, 0, err
		}
		events = append(events, cloneEvent(event))
	}
	return events, current, nil
}

func (s *etcdStore) compact(ctx context.Context, before uint64) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	return s.deleteEventsBefore(ctx, before)
}

func (s *etcdStore) subscribe(ctx context.Context, resource string) (<-chan struct{}, func(), error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, nil, err
	}
	key := s.notifyKey(resource)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	watchCtx, cancelWatch := context.WithCancel(ctx)
	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		revision := resp.Header.Revision + 1
		backoff := 50 * time.Millisecond
		for {
			if watchCtx.Err() != nil {
				return
			}
			watchCh := s.client.Watch(watchCtx, key, clientv3.WithRev(revision))
			restart := false
			for !restart {
				select {
				case <-watchCtx.Done():
					return
				case watchResp, ok := <-watchCh:
					if !ok {
						restart = true
						break
					}
					if watchResp.Header.Revision > 0 {
						revision = watchResp.Header.Revision + 1
					}
					if watchResp.Err() != nil {
						restart = true
						break
					}
					if len(watchResp.Events) > 0 {
						select {
						case out <- struct{}{}:
						default:
						}
					}
				}
			}
			timer := time.NewTimer(backoff)
			select {
			case <-watchCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < time.Second {
				backoff *= 2
			}
		}
	}()
	return out, cancelWatch, nil
}

func (s *etcdStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *etcdStore) ensureOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *etcdStore) currentRV(ctx context.Context) (uint64, error) {
	return s.readUint(ctx, s.metaKey("rv"))
}

func (s *etcdStore) readMetaRV(ctx context.Context) (uint64, string, int64, error) {
	resp, err := s.client.Get(ctx, s.metaKey("rv"), clientv3.WithLimit(1))
	if err != nil {
		return 0, "", 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, "", 0, nil
	}
	raw := string(resp.Kvs[0].Value)
	return parseStoredRV(raw), raw, resp.Kvs[0].Version, nil
}

func (s *etcdStore) readUint(ctx context.Context, key string) (uint64, error) {
	resp, err := s.client.Get(ctx, key, clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	return parseStoredRV(string(resp.Kvs[0].Value)), nil
}

func (s *etcdStore) metaRVCompare(raw string, version int64) clientv3.Cmp {
	if version == 0 {
		return clientv3.Compare(clientv3.Version(s.metaKey("rv")), "=", 0)
	}
	return clientv3.Compare(clientv3.Value(s.metaKey("rv")), "=", raw)
}

func (s *etcdStore) enforceRetention(ctx context.Context, retention int) error {
	prefix := s.eventPrefix("")
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return err
	}
	if len(resp.Kvs) <= retention {
		return nil
	}
	cutoff := parseRVKey(string(resp.Kvs[len(resp.Kvs)-retention-1].Key))
	return s.deleteEventsBefore(ctx, cutoff)
}

func (s *etcdStore) deleteEventsBefore(ctx context.Context, before uint64) error {
	prefix := s.eventPrefix("")
	resp, err := s.client.Get(
		ctx,
		prefix,
		clientv3.WithRange(prefix+rvKey(before+1)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return err
	}
	ops := make([]clientv3.Op, 0, len(resp.Kvs)*2+1)
	for _, kv := range resp.Kvs {
		var event resourceEvent
		if err := json.Unmarshal(kv.Value, &event); err != nil {
			return err
		}
		rv := parseStoredRV(event.ResourceVersion)
		ops = append(ops,
			clientv3.OpDelete(s.eventAllKey(rv)),
			clientv3.OpDelete(s.eventResourceKey(event.Ref.Resource, rv)),
		)
	}
	currentCompacted, err := s.readUint(ctx, s.metaKey("compacted-rv"))
	if err != nil {
		return err
	}
	if before > currentCompacted {
		ops = append(ops, clientv3.OpPut(s.metaKey("compacted-rv"), formatRV(before)))
	}
	for len(ops) > 0 {
		batch := ops
		if len(batch) > 120 {
			batch = batch[:120]
		}
		if _, err := s.client.Txn(ctx).Then(batch...).Commit(); err != nil {
			return err
		}
		ops = ops[len(batch):]
	}
	return nil
}

func (s *etcdStore) metaKey(name string) string {
	return s.prefix + "meta/" + name
}

func (s *etcdStore) objectKey(ref objectRef) string {
	return s.prefix + "objects/" + ref.Resource + "/" + ref.Name
}

func (s *etcdStore) objectPrefix(resource string) string {
	if resource == "" {
		return s.prefix + "objects/"
	}
	return s.prefix + "objects/" + resource + "/"
}

func (s *etcdStore) eventPrefix(resource string) string {
	if resource == "" {
		return s.prefix + "events/all/"
	}
	return s.prefix + "events/resources/" + resource + "/"
}

func (s *etcdStore) eventAllKey(rv uint64) string {
	return s.eventPrefix("") + rvKey(rv)
}

func (s *etcdStore) eventResourceKey(resource string, rv uint64) string {
	return s.eventPrefix(resource) + rvKey(rv)
}

func (s *etcdStore) notifyKey(resource string) string {
	if resource == "" {
		return s.prefix + "notify/all"
	}
	return s.prefix + "notify/resources/" + resource
}
