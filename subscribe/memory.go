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
	closed   bool
	isRoot   bool
}

// NewMemorySubscriber 创建新的内存订阅者
func NewMemorySubscriber() *MemorySubscriber {
	return &MemorySubscriber{
		channels: make(map[string][]chan string),
		mu:       &sync.RWMutex{},
		closed:   false,
		isRoot:   true,
	}
}

func (m *MemorySubscriber) Child(prefix string) Subscriber {
	if prefix == "" {
		return m
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return &MemorySubscriber{
			prefix:   m.buildFullKey(prefix),
			mu:       m.mu,
			channels: m.channels,
			closed:   true,
			isRoot:   false,
		}
	}

	return &MemorySubscriber{
		prefix:   m.buildFullKey(prefix),
		mu:       m.mu,
		channels: m.channels,
		closed:   false,
		isRoot:   false,
	}
}

// buildFullKey 构建完整key
func (m *MemorySubscriber) buildFullKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if m.prefix == "" {
		return key
	}
	return m.prefix + "/" + key
}

// Close 关闭所有订阅通道
func (m *MemorySubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 只有 root 节点才能执行关闭操作
	if !m.isRoot {
		return nil
	}

	if m.closed {
		return nil
	}

	m.closed = true

	// 关闭所有 channel
	for key, channels := range m.channels {
		for _, ch := range channels {
			close(ch)
		}
		delete(m.channels, key)
	}

	return nil
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

	m.channels[key] = append(m.channels[key], ch)
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

	if channels, exists := m.channels[key]; exists {
		for i, c := range channels {
			if c == ch {
				// 从切片中移除channel
				m.channels[key] = append(channels[:i], channels[i+1:]...)
				close(ch)
				break
			}
		}
		// 如果该key没有订阅者了，删除key
		if len(m.channels[key]) == 0 {
			delete(m.channels, key)
		}
	}
}
