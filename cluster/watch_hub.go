package cluster

import "sync"

type watchHub struct {
	mu         sync.Mutex
	nextID     uint64
	closed     bool
	all        map[uint64]chan struct{}
	byResource map[string]map[uint64]chan struct{}
}

func newWatchHub() *watchHub {
	return &watchHub{
		all:        make(map[uint64]chan struct{}),
		byResource: make(map[string]map[uint64]chan struct{}),
	}
}

func (h *watchHub) subscribe(resource string) (<-chan struct{}, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan struct{}, 1)
	if h.closed {
		close(ch)
		return ch, func() {}, nil
	}
	h.nextID++
	id := h.nextID
	if resource == "" {
		h.all[id] = ch
	} else {
		if h.byResource[resource] == nil {
			h.byResource[resource] = make(map[uint64]chan struct{})
		}
		h.byResource[resource][id] = ch
	}
	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if resource == "" {
			if registered, ok := h.all[id]; ok {
				delete(h.all, id)
				close(registered)
			}
			return
		}
		if registered, ok := h.byResource[resource][id]; ok {
			delete(h.byResource[resource], id)
			close(registered)
		}
		if len(h.byResource[resource]) == 0 {
			delete(h.byResource, resource)
		}
	}
	return ch, cancel, nil
}

func (h *watchHub) notify(resource string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ch := range h.all {
		notifyChannel(ch)
	}
	for _, ch := range h.byResource[resource] {
		notifyChannel(ch)
	}
}

func (h *watchHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, ch := range h.all {
		delete(h.all, id)
		close(ch)
	}
	for resource, watchers := range h.byResource {
		for id, ch := range watchers {
			delete(watchers, id)
			close(ch)
		}
		delete(h.byResource, resource)
	}
}

func notifyChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
