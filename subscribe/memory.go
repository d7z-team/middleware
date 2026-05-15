package subscribe

import (
	"context"
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
	state  *subscriptionState
	cancel context.CancelFunc

	owner *MemorySubscriber
	once  sync.Once
}

func (s *memorySubscription) Events() <-chan Event { return s.state.Events() }

func (s *memorySubscription) Errors() <-chan error { return s.state.Errors() }

func (s *memorySubscription) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	s.once.Do(func() {
		s.cancel()
		s.state.closeChannels()
	})
	s.owner.mu.Lock()
	s.owner.removeSubscriptionLocked(s.key, s)
	s.owner.mu.Unlock()
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
	childPrefix := subscriberChildPrefix(m.prefix, paths...)
	if childPrefix == m.prefix {
		return m
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	child := &MemorySubscriber{
		prefix:    childPrefix,
		mu:        m.mu,
		channels:  m.channels,
		parent:    m,
		localSubs: make([]*memorySubscription, 0),
	}
	m.children = append(m.children, child)
	return child
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
	subs := append([]*memorySubscription(nil), m.localSubs...)
	m.localSubs = nil
	for _, sub := range subs {
		sub := sub
		sub.once.Do(func() {
			sub.cancel()
			sub.state.closeChannels()
			m.removeSubscriptionLocked(sub.key, sub)
		})
	}
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
}

func (m *MemorySubscriber) Publish(ctx context.Context, key, data string) error {
	key = buildSubscriberKey(m.prefix, key)

	m.mu.RLock()
	if m.isClosedLocked() {
		m.mu.RUnlock()
		return nil
	}
	subs := append([]*memorySubscription(nil), m.channels[key]...)
	m.mu.RUnlock()

	event := Event{Key: key, Value: data}
	for _, sub := range subs {
		if err := sub.state.trySendEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemorySubscriber) Subscribe(ctx context.Context, key string) (Subscription, error) {
	key = buildSubscriberKey(m.prefix, key)
	subCtx, cancel := context.WithCancel(ctx)
	sub := &memorySubscription{
		key:    key,
		state:  newSubscriptionState(),
		cancel: cancel,
		owner:  m,
	}

	m.mu.Lock()
	if m.isClosedLocked() {
		m.mu.Unlock()
		sub.cancel()
		sub.state.closeChannels()
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
