package cluster

import (
	"context"
	"sync"
	"time"
)

type memoryStore struct {
	prefix    string
	mu        sync.RWMutex
	objects   map[string]map[string]Unstructured
	events    []resourceEvent
	rv        uint64
	compacted uint64
	retention int
	hub       *watchHub
	closed    bool
}

var memoryNodeLeases = struct {
	sync.Mutex
	records map[string]nodeLeaseRecord
}{
	records: make(map[string]nodeLeaseRecord),
}

func newMemoryStore(options Options) *memoryStore {
	return &memoryStore{
		prefix:    normalizeStorePrefix(options.Prefix),
		objects:   make(map[string]map[string]Unstructured),
		events:    make([]resourceEvent, 0),
		retention: options.EventRetentionCount,
		hub:       newWatchHub(),
	}
}

func (s *memoryStore) get(ctx context.Context, ref objectRef) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	resource := s.objects[ref.Resource]
	if resource == nil {
		return nil, ErrNotFound
	}
	obj, ok := resource[objectStorageKey(ref)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneUnstructuredPtr(&obj), nil
}

func (s *memoryStore) list(ctx context.Context, scope resourceScope) ([]Unstructured, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	out := make([]Unstructured, 0)
	if scope.Resource != "" {
		for _, obj := range s.objects[scope.Resource] {
			if objectMatchesScope(obj, scope) {
				out = append(out, cloneUnstructured(obj))
			}
		}
		return out, s.rv, nil
	}
	for _, byName := range s.objects {
		for _, obj := range byName {
			if objectMatchesScope(obj, scope) {
				out = append(out, cloneUnstructured(obj))
			}
		}
	}
	return out, s.rv, nil
}

func (s *memoryStore) commit(ctx context.Context, req commitRequest) (*Unstructured, resourceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, resourceEvent{}, err
	}
	s.mu.Lock()
	obj, event, err := s.commitLocked(req)
	s.mu.Unlock()
	if err != nil {
		return nil, resourceEvent{}, err
	}
	s.hub.notify(req.Ref)
	return obj, event, nil
}

func (s *memoryStore) commitLocked(req commitRequest) (*Unstructured, resourceEvent, error) {
	if s.closed {
		return nil, resourceEvent{}, ErrClosed
	}
	resource := s.objects[req.Ref.Resource]
	current, exists := resource[objectStorageKey(req.Ref)]
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

	s.rv++
	eventObj := cloneUnstructured(*req.Object)
	eventObj.Metadata.ResourceVersion = formatRV(s.rv)
	if req.Op != commitDelete {
		if resource == nil {
			resource = make(map[string]Unstructured)
			s.objects[req.Ref.Resource] = resource
		}
		resource[objectStorageKey(req.Ref)] = eventObj
	} else {
		delete(resource, objectStorageKey(req.Ref))
		if len(resource) == 0 {
			delete(s.objects, req.Ref.Resource)
		}
	}
	event := newStoreEvent(req, s.rv, &eventObj)
	s.events = append(s.events, event)
	return cloneUnstructuredPtr(&eventObj), event, nil
}

func (s *memoryStore) eventsAfter(ctx context.Context, after uint64, scope resourceScope, limit int) ([]resourceEvent, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	if after < s.compacted {
		return nil, s.rv, ErrResourceVersionTooOld
	}
	out := make([]resourceEvent, 0, min(limit, len(s.events)))
	for _, event := range s.events {
		if parseStoredRV(event.ResourceVersion) <= after {
			continue
		}
		if !eventMatchesScope(event, scope) {
			continue
		}
		out = append(out, cloneEvent(event))
		if len(out) >= limit {
			break
		}
	}
	return out, s.rv, nil
}

func (s *memoryStore) cleanupEvents(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.enforceRetentionLocked()
	return nil
}

func (s *memoryStore) subscribe(ctx context.Context, scope resourceScope) (<-chan struct{}, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.hub.subscribe(scope)
}

func (s *memoryStore) acquireNode(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token, err := randomToken("node")
	if err != nil {
		return "", err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	now := time.Now().UTC()
	if record, ok := memoryNodeLeases.records[key]; ok {
		if record.ExpiresAt.After(now) {
			return "", ErrNodeAlreadyExists
		}
		delete(memoryNodeLeases.records, key)
	}
	memoryNodeLeases.records[key] = nodeLeaseRecord{
		Token:     token,
		ExpiresAt: now.Add(ttl),
	}
	return token, nil
}

func (s *memoryStore) renewNode(ctx context.Context, name, token string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	record, ok := memoryNodeLeases.records[key]
	now := time.Now().UTC()
	if !ok || record.Token != token || !record.ExpiresAt.After(now) {
		return ErrNodeLeaseLost
	}
	record.ExpiresAt = now.Add(ttl)
	memoryNodeLeases.records[key] = record
	return nil
}

func (s *memoryStore) releaseNode(ctx context.Context, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	if record, ok := memoryNodeLeases.records[key]; ok && record.Token == token {
		delete(memoryNodeLeases.records, key)
	}
	return nil
}

func (s *memoryStore) close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.hub.close()
	return nil
}

func (s *memoryStore) nodeLeaseKey(name string) string {
	return s.prefix + "leases/nodes/" + name
}

func (s *memoryStore) enforceRetentionLocked() {
	if s.rv <= uint64(s.retention) {
		return
	}
	before := s.rv - uint64(s.retention)
	s.compacted = max(s.compacted, before)
	kept := s.events[:0]
	for _, event := range s.events {
		if parseStoredRV(event.ResourceVersion) > before {
			kept = append(kept, event)
		}
	}
	s.events = kept
}
