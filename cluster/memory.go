package cluster

import (
	"context"
	"sync"
)

type memoryStore struct {
	mu        sync.RWMutex
	objects   map[string]map[string]Unstructured
	events    []resourceEvent
	rv        uint64
	compacted uint64
	retention int
	hub       *watchHub
	closed    bool
}

func newMemoryStore(options Options) *memoryStore {
	return &memoryStore{
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
	obj, ok := resource[ref.Name]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneUnstructuredPtr(&obj), nil
}

func (s *memoryStore) list(ctx context.Context, resource string) ([]Unstructured, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	out := make([]Unstructured, 0)
	if resource != "" {
		for _, obj := range s.objects[resource] {
			out = append(out, cloneUnstructured(obj))
		}
		return out, s.rv, nil
	}
	for _, byName := range s.objects {
		for _, obj := range byName {
			out = append(out, cloneUnstructured(obj))
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
	s.hub.notify(req.Ref.Resource)
	return obj, event, nil
}

func (s *memoryStore) commitLocked(req commitRequest) (*Unstructured, resourceEvent, error) {
	if s.closed {
		return nil, resourceEvent{}, ErrClosed
	}
	resource := s.objects[req.Ref.Resource]
	current, exists := resource[req.Ref.Name]
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
		resource[req.Ref.Name] = eventObj
	} else {
		delete(resource, req.Ref.Name)
		if len(resource) == 0 {
			delete(s.objects, req.Ref.Resource)
		}
	}
	event := newStoreEvent(req, s.rv, &eventObj)
	s.events = append(s.events, event)
	s.enforceRetentionLocked()
	return cloneUnstructuredPtr(&eventObj), event, nil
}

func (s *memoryStore) eventsAfter(ctx context.Context, after uint64, resource string, limit int) ([]resourceEvent, uint64, error) {
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
		if resource != "" && event.Ref.Resource != resource {
			continue
		}
		out = append(out, cloneEvent(event))
		if len(out) >= limit {
			break
		}
	}
	return out, s.rv, nil
}

func (s *memoryStore) compact(ctx context.Context, before uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.compacted = max(s.compacted, before)
	kept := s.events[:0]
	for _, event := range s.events {
		if parseStoredRV(event.ResourceVersion) > before {
			kept = append(kept, event)
		}
	}
	s.events = kept
	return nil
}

func (s *memoryStore) subscribe(ctx context.Context, resource string) (<-chan struct{}, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.hub.subscribe(resource)
}

func (s *memoryStore) close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.hub.close()
	return nil
}

func (s *memoryStore) enforceRetentionLocked() {
	if len(s.events) <= s.retention {
		return
	}
	remove := len(s.events) - s.retention
	for i := 0; i < remove; i++ {
		s.compacted = max(s.compacted, parseStoredRV(s.events[i].ResourceVersion))
	}
	copy(s.events, s.events[remove:])
	s.events = s.events[:len(s.events)-remove]
}
