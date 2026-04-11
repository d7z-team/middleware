package subscribe

import (
	"context"
	"strings"
	"sync"
)

type MemorySubscriber struct {
	mu       *sync.RWMutex
	prefix   string
	channels map[string][]chan string
	parent   *MemorySubscriber

	localChannels []struct {
		key string
		ch  chan string
	}

	children    []*MemorySubscriber
	localClosed bool
}

// NewMemorySubscriber creates an in-memory subscriber.
//
// Example:
//
//	sub := NewMemorySubscriber()
func NewMemorySubscriber() *MemorySubscriber {
	return &MemorySubscriber{
		channels: make(map[string][]chan string),
		mu:       &sync.RWMutex{},
		localChannels: make([]struct {
			key string
			ch  chan string
		}, 0),
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
		prefix:   m.prefix + strings.Join(keys, "/") + "/",
		mu:       m.mu,
		channels: m.channels,
		parent:   m,
		localChannels: make([]struct {
			key string
			ch  chan string
		}, 0),
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
	for _, item := range m.localChannels {
		m.removeChannelFromGlobal(item.key, item.ch)
		close(item.ch)
	}
	m.localChannels = nil
}

func (m *MemorySubscriber) isClosedLocked() bool {
	for current := m; current != nil; current = current.parent {
		if current.localClosed {
			return true
		}
	}
	return false
}

func (m *MemorySubscriber) removeChannelFromGlobal(key string, targetCh chan string) {
	if channels, exists := m.channels[key]; exists {
		for i, ch := range channels {
			if ch == targetCh {
				// Remove from slice
				m.channels[key] = append(channels[:i], channels[i+1:]...)
				break
			}
		}
		if len(m.channels[key]) == 0 {
			delete(m.channels, key)
		}
	}
}

func (m *MemorySubscriber) Publish(ctx context.Context, key, data string) error {
	key = m.buildFullKey(key)

	m.mu.RLock()

	if m.isClosedLocked() {
		m.mu.RUnlock()
		return nil
	}

	var channels []chan string
	if existingChannels, exists := m.channels[key]; exists {
		channels = make([]chan string, len(existingChannels))
		copy(channels, existingChannels)
	}

	m.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- data:
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return nil
}

func (m *MemorySubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	key = m.buildFullKey(key)
	ch := make(chan string, 1000)

	m.mu.Lock()

	if m.isClosedLocked() {
		m.mu.Unlock()
		close(ch)
		return ch, nil
	}

	m.channels[key] = append(m.channels[key], ch)

	m.localChannels = append(m.localChannels, struct {
		key string
		ch  chan string
	}{key, ch})

	m.mu.Unlock()

	go m.monitorContext(ctx, key, ch)

	return ch, nil
}

func (m *MemorySubscriber) monitorContext(ctx context.Context, key string, ch chan string) {
	<-ctx.Done()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isClosedLocked() {
		return
	}

	m.removeChannelFromGlobal(key, ch)

	for i, item := range m.localChannels {
		if item.ch == ch {
			m.localChannels = append(m.localChannels[:i], m.localChannels[i+1:]...)
			close(ch)
			break
		}
	}
}
