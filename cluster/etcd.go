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

func (s *etcdStore) list(ctx context.Context, scope resourceScope) ([]Unstructured, uint64, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, 0, err
	}
	prefix := s.objectPrefix(scope)
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
		if objectMatchesScope(obj, scope) {
			objects = append(objects, cloneUnstructured(obj))
		}
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
			clientv3.OpPut(s.notifyKey(resourceScope{}), formatRV(nextRV)),
			clientv3.OpPut(s.notifyKey(resourceScope{Resource: req.Ref.Resource, AllNamespaces: true}), formatRV(nextRV)),
		}
		if req.Ref.Namespace != "" {
			ops = append(ops,
				clientv3.OpPut(s.eventNamespaceKey(req.Ref, nextRV), string(eventRaw)),
				clientv3.OpPut(s.notifyKey(resourceScope{Resource: req.Ref.Resource, Namespace: req.Ref.Namespace}), formatRV(nextRV)),
			)
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

func (s *etcdStore) eventsAfter(ctx context.Context, after uint64, scope resourceScope, limit int) ([]resourceEvent, uint64, error) {
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
	prefix := s.eventPrefix(scope)
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	if scope.Resource != "" && scope.Namespace == "" {
		rangeEnd = prefix + ":"
	}
	resp, err := s.client.Get(
		ctx,
		prefix+rvKey(after+1),
		clientv3.WithRange(rangeEnd),
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

func (s *etcdStore) cleanupEvents(ctx context.Context) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	current, err := s.currentRV(ctx)
	if err != nil {
		return err
	}
	if current <= uint64(s.retention) {
		return nil
	}
	before := current - uint64(s.retention)
	if err := s.advanceCompactedRV(ctx, before); err != nil {
		return err
	}
	return s.deleteEventsBefore(ctx, before)
}

func (s *etcdStore) subscribe(ctx context.Context, scope resourceScope) (<-chan struct{}, func(), error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, nil, err
	}
	key := s.notifyKey(scope)
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

func (s *etcdStore) acquireNode(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return "", err
	}
	token, err := randomToken("node")
	if err != nil {
		return "", err
	}
	lease, err := s.client.Grant(ctx, etcdLeaseSeconds(ttl))
	if err != nil {
		return "", err
	}
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(s.nodeLeaseKey(name)), "=", 0)).
		Then(clientv3.OpPut(s.nodeLeaseKey(name), token, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return "", err
	}
	if !resp.Succeeded {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return "", ErrNodeAlreadyExists
	}
	return token, nil
}

func (s *etcdStore) renewNode(ctx context.Context, name, token string, ttl time.Duration) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	lease, err := s.client.Grant(ctx, etcdLeaseSeconds(ttl))
	if err != nil {
		return err
	}
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(s.nodeLeaseKey(name)), "=", token)).
		Then(clientv3.OpPut(s.nodeLeaseKey(name), token, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return err
	}
	if !resp.Succeeded {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return ErrNodeLeaseLost
	}
	return nil
}

func (s *etcdStore) releaseNode(ctx context.Context, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil
	}
	_, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(s.nodeLeaseKey(name)), "=", token)).
		Then(clientv3.OpDelete(s.nodeLeaseKey(name))).
		Commit()
	return err
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

func (s *etcdStore) deleteEventsBefore(ctx context.Context, before uint64) error {
	prefix := s.eventPrefix(resourceScope{})
	end := prefix + rvKey(before+1)
	for {
		resp, err := s.client.Get(
			ctx,
			prefix,
			clientv3.WithRange(end),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			clientv3.WithLimit(60),
		)
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}
		ops := make([]clientv3.Op, 0, len(resp.Kvs)*3)
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
			if event.Ref.Namespace != "" {
				ops = append(ops, clientv3.OpDelete(s.eventNamespaceKey(event.Ref, rv)))
			}
		}
		if _, err := s.client.Txn(ctx).Then(ops...).Commit(); err != nil {
			return err
		}
	}
}

func (s *etcdStore) advanceCompactedRV(ctx context.Context, before uint64) error {
	key := s.metaKey("compacted-rv")
	for {
		resp, err := s.client.Get(ctx, key, clientv3.WithLimit(1))
		if err != nil {
			return err
		}
		var current uint64
		var cmp clientv3.Cmp
		if len(resp.Kvs) == 0 {
			cmp = clientv3.Compare(clientv3.Version(key), "=", 0)
		} else {
			current = parseStoredRV(string(resp.Kvs[0].Value))
			if before <= current {
				return nil
			}
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)
		}
		txnResp, err := s.client.Txn(ctx).If(cmp).Then(clientv3.OpPut(key, formatRV(before))).Commit()
		if err != nil {
			return err
		}
		if txnResp.Succeeded {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *etcdStore) metaKey(name string) string {
	return s.prefix + "meta/" + name
}

func (s *etcdStore) objectKey(ref objectRef) string {
	if ref.Namespace != "" {
		return s.prefix + "objects/" + ref.Resource + "/namespaces/" + ref.Namespace + "/" + ref.Name
	}
	return s.prefix + "objects/" + ref.Resource + "/" + ref.Name
}

func (s *etcdStore) objectPrefix(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "objects/"
	}
	if scope.Namespace != "" {
		return s.prefix + "objects/" + scope.Resource + "/namespaces/" + scope.Namespace + "/"
	}
	return s.prefix + "objects/" + scope.Resource + "/"
}

func (s *etcdStore) eventPrefix(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "events/all/"
	}
	if scope.Namespace != "" {
		return s.prefix + "events/resources/" + scope.Resource + "/namespaces/" + scope.Namespace + "/"
	}
	return s.prefix + "events/resources/" + scope.Resource + "/"
}

func (s *etcdStore) eventAllKey(rv uint64) string {
	return s.eventPrefix(resourceScope{}) + rvKey(rv)
}

func (s *etcdStore) eventResourceKey(resource string, rv uint64) string {
	return s.eventPrefix(resourceScope{Resource: resource, AllNamespaces: true}) + rvKey(rv)
}

func (s *etcdStore) eventNamespaceKey(ref objectRef, rv uint64) string {
	return s.eventPrefix(resourceScope{Resource: ref.Resource, Namespace: ref.Namespace}) + rvKey(rv)
}

func (s *etcdStore) notifyKey(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "notify/all"
	}
	if scope.Namespace != "" {
		return s.prefix + "notify/resources/" + scope.Resource + "/namespaces/" + scope.Namespace
	}
	return s.prefix + "notify/resources/" + scope.Resource
}

func (s *etcdStore) nodeLeaseKey(name string) string {
	return s.prefix + "leases/nodes/" + name
}

func etcdLeaseSeconds(ttl time.Duration) int64 {
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}
