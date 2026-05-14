package subscribe

import (
	"context"
	"strings"
	"sync"
)

type MemorySubscriber struct {
	mu       *sync.RWMutex
	prefix   string
	channels map[string][]*memorySubscription
	parent   *MemorySubscriber

	localSubs []*memorySubscription
	children  []*MemorySubscriber

	localClosed bool
}

type memorySubscription struct {
	key    string
	events chan Event
	errors chan error
	cancel context.CancelFunc

	owner *MemorySubscriber
	once  sync.Once
}

func (s *memorySubscription) Events() <-chan Event { return s.events }

func (s *memorySubscription) Errors() <-chan error { return s.errors }

func (s *memorySubscription) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	s.once.Do(func() {
		s.cancel()
		s.owner.mu.Lock()
		defer s.owner.mu.Unlock()
		s.owner.removeSubscriptionLocked(s.key, s)
	})
	return nil
}

func NewMemorySubscriber() *MemorySubscriber {
	return &MemorySubscriber{
		channels:  make(map[string][]*memorySubscription),
		mu:        &sync.RWMutex{},
		localSubs: make([]*memorySubscription, 0),
	}
}

func (m *MemorySubscriber) Child(paths ...string) Subscriber {
	if len(paths) == 0 {
		return m
	}
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		keys = append(keys, path)
	}
	if len(keys) == 0 {
		return m
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	child := &MemorySubscriber{
		prefix:    m.prefix + strings.Join(keys, "/") + "/",
		mu:        m.mu,
		channels:  m.channels,
		parent:    m,
		localSubs: make([]*memorySubscription, 0),
	}
	m.children = append(m.children, child)
	return child
}

func (m *MemorySubscriber) buildFullKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if m.prefix == "" {
		return key
	}
	if strings.HasSuffix(m.prefix, "/") {
		return m.prefix + key
	}
	return m.prefix + "/" + key
}

func (m *MemorySubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeLocked()
	return nil
}

func (m *MemorySubscriber) closeLocked() {
	if m.localClosed {
		return
	}
	m.localClosed = true
	for _, child := range m.children {
		child.closeLocked()
	}
	for _, sub := range m.localSubs {
		sub.once.Do(func() {
			sub.cancel()
			m.removeSubscriptionLocked(sub.key, sub)
		})
	}
	m.localSubs = nil
}

func (m *MemorySubscriber) isClosedLocked() bool {
	for current := m; current != nil; current = current.parent {
		if current.localClosed {
			return true
		}
	}
	return false
}

func (m *MemorySubscriber) removeSubscriptionLocked(key string, target *memorySubscription) {
	subs := m.channels[key]
	for i, sub := range subs {
		if sub == target {
			m.channels[key] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(m.channels[key]) == 0 {
		delete(m.channels, key)
	}
	for i, sub := range m.localSubs {
		if sub == target {
			m.localSubs = append(m.localSubs[:i], m.localSubs[i+1:]...)
			break
		}
	}
	close(target.events)
	close(target.errors)
}

func (m *MemorySubscriber) Publish(ctx context.Context, key, data string) error {
	key = m.buildFullKey(key)

	m.mu.RLock()
	if m.isClosedLocked() {
		m.mu.RUnlock()
		return nil
	}
	subs := append([]*memorySubscription(nil), m.channels[key]...)
	m.mu.RUnlock()

	for _, sub := range subs {
		event := Event{Key: key, Value: data}
		select {
		case sub.events <- event:
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (m *MemorySubscriber) Subscribe(ctx context.Context, key string) (Subscription, error) {
	key = m.buildFullKey(key)
	events := make(chan Event, 1000)
	errors := make(chan error, 16)

	subCtx, cancel := context.WithCancel(ctx)
	sub := &memorySubscription{
		key:    key,
		events: events,
		errors: errors,
		cancel: cancel,
		owner:  m,
	}

	m.mu.Lock()
	if m.isClosedLocked() {
		m.mu.Unlock()
		close(events)
		close(errors)
		return sub, nil
	}
	m.channels[key] = append(m.channels[key], sub)
	m.localSubs = append(m.localSubs, sub)
	m.mu.Unlock()

	go m.monitorSubscription(subCtx, sub)
	return sub, nil
}

func (m *MemorySubscriber) monitorSubscription(ctx context.Context, sub *memorySubscription) {
	<-ctx.Done()
	_ = sub.Close()
}
