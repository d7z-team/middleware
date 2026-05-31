package cluster

import "sync"

type watchHub struct {
	mu       sync.Mutex
	nextID   uint64
	closed   bool
	watchers map[string]map[uint64]chan struct{}
}

func newWatchHub() *watchHub {
	return &watchHub{
		watchers: make(map[string]map[uint64]chan struct{}),
	}
}

func (h *watchHub) subscribe(scope resourceScope) (<-chan struct{}, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan struct{}, 1)
	if h.closed {
		close(ch)
		return ch, func() {}, nil
	}
	h.nextID++
	id := h.nextID
	key := watchKey(scope.Resource, scope.Namespace)
	if h.watchers[key] == nil {
		h.watchers[key] = make(map[uint64]chan struct{})
	}
	h.watchers[key][id] = ch
	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if registered, ok := h.watchers[key][id]; ok {
			delete(h.watchers[key], id)
			close(registered)
		}
		if len(h.watchers[key]) == 0 {
			delete(h.watchers, key)
		}
	}
	return ch, cancel, nil
}

func (h *watchHub) notify(ref objectRef) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.watchers[watchKey(ref.Resource, "")] {
		notifyChannel(ch)
	}
	if ref.Namespace != "" {
		for _, ch := range h.watchers[watchKey(ref.Resource, ref.Namespace)] {
			notifyChannel(ch)
		}
	}
}

func (h *watchHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for key, watchers := range h.watchers {
		for id, ch := range watchers {
			delete(watchers, id)
			close(ch)
		}
		delete(h.watchers, key)
	}
}

func notifyChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func watchKey(resource, namespace string) string {
	if namespace == "" {
		return resource
	}
	return resource + "\x00" + namespace
}
