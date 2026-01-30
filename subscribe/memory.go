package subscribe

import (
	"context"
	"strings"
	"sync"
)

// MemorySubscriber 修复后的内存订阅者实现
type MemorySubscriber struct {
	mu       *sync.RWMutex
	prefix   string
	channels map[string][]chan string

	// localChannels tracks channels created by this specific subscriber instance
	// to ensure we only close/clean up our own subscriptions.
	localChannels []struct {
		key string
		ch  chan string
	}

	closed bool
}

// NewMemorySubscriber 创建新的内存订阅者
func NewMemorySubscriber() *MemorySubscriber {
	return &MemorySubscriber{
		channels: make(map[string][]chan string),
		mu:       &sync.RWMutex{},
		localChannels: make([]struct {
			key string
			ch  chan string
		}, 0),
		closed: false,
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
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a new instance sharing the global state (channels, mu)
	// but with its own isolated localChannels list.
	return &MemorySubscriber{
		prefix:   m.prefix + strings.Join(keys, "/") + "/",
		mu:       m.mu,
		channels: m.channels,
		localChannels: make([]struct {
			key string
			ch  chan string
		}, 0),
		closed: m.closed,
	}
}

// buildFullKey 构建完整key
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

// Close 关闭当前订阅者的所有订阅通道
func (m *MemorySubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	// 只关闭和清理当前实例创建的 channel
	for _, item := range m.localChannels {
		m.removeChannelFromGlobal(item.key, item.ch)
		close(item.ch)
	}

	// 清空本地记录
	m.localChannels = nil

	return nil
}

// removeChannelFromGlobal is a helper to remove a specific channel from the global map.
// It assumes the caller holds the lock.
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

// Publish 发布消息 - 修复：不在锁内发送消息
func (m *MemorySubscriber) Publish(ctx context.Context, key, data string) error {
	key = m.buildFullKey(key)

	m.mu.RLock()

	// 检查是否已关闭
	if m.closed {
		m.mu.RUnlock()
		return nil
	}

	// 复制 channels 引用，避免在锁内操作
	var channels []chan string
	if existingChannels, exists := m.channels[key]; exists {
		channels = make([]chan string, len(existingChannels))
		copy(channels, existingChannels)
	}

	m.mu.RUnlock()

	// 在锁外发送消息
	for _, ch := range channels {
		select {
		case ch <- data:
			// 成功发送
		case <-ctx.Done():
			return ctx.Err()
		default:
			// channel已满，跳过
		}
	}

	return nil
}

// Subscribe 订阅消息
func (m *MemorySubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	key = m.buildFullKey(key)
	ch := make(chan string, 1000) // 增大缓冲区

	m.mu.Lock()

	// 检查是否已关闭
	if m.closed {
		m.mu.Unlock()
		close(ch)
		return ch, nil
	}

	// 添加到全局 map
	m.channels[key] = append(m.channels[key], ch)

	// 添加到本地记录
	m.localChannels = append(m.localChannels, struct {
		key string
		ch  chan string
	}{key, ch})

	m.mu.Unlock()

	// 监听上下文取消，清理资源
	go m.monitorContext(ctx, key, ch)

	return ch, nil
}

// monitorContext 监听上下文取消并清理资源
func (m *MemorySubscriber) monitorContext(ctx context.Context, key string, ch chan string) {
	<-ctx.Done()

	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果已关闭，不需要清理
	if m.closed {
		return
	}

	// 清理全局 map
	m.removeChannelFromGlobal(key, ch)

	// 清理本地 localChannels
	// 这是一个O(N)操作，但通常每个订阅者的订阅数量不会太多
	for i, item := range m.localChannels {
		if item.ch == ch {
			m.localChannels = append(m.localChannels[:i], m.localChannels[i+1:]...)
			close(ch)
			break
		}
	}
}
