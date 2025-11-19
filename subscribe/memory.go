package subscribe

import (
	"context"
	"strings"
	"sync"
)

// MemorySubscriber 精简的内存订阅者实现
type MemorySubscriber struct {
	mu       *sync.RWMutex
	prefix   string
	channels map[string][]chan string
	closed   bool
}

// NewMemorySubscriber 创建新的内存订阅者
func NewMemorySubscriber() *MemorySubscriber {
	return &MemorySubscriber{
		channels: make(map[string][]chan string),
		mu:       &sync.RWMutex{},
		closed:   false,
	}
}

func (m *MemorySubscriber) Child(prefix string) Subscriber {
	if prefix == "" {
		return m
	}
	prefix = m.prefix + strings.Trim(m.prefix, "/") + "/"
	return &MemorySubscriber{
		prefix:   prefix,
		mu:       m.mu,
		channels: m.channels,
		closed:   m.closed, // 共享 closed 状态
	}
}

// Close 关闭所有订阅通道，只能被 root 调用
func (m *MemorySubscriber) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 只有 root 节点（prefix 为空）才能执行关闭操作
	if m.prefix != "" {
		return nil
	}

	if m.closed {
		return nil
	}

	m.closed = true

	// 关闭所有 channel
	for _, channels := range m.channels {
		for _, ch := range channels {
			close(ch)
		}
	}

	// 清空 channels
	m.channels = make(map[string][]chan string)

	return nil
}

// Publish 发布消息
func (m *MemorySubscriber) Publish(ctx context.Context, key string, data string) error {
	key = m.prefix + key
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 检查是否已关闭
	if m.closed {
		return nil
	}

	if channels, exists := m.channels[key]; exists {
		// 在锁内发送，确保channel状态一致
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
	}

	return nil
}

// Subscribe 订阅消息
func (m *MemorySubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	key = m.prefix + key
	ch := make(chan string, 100) // 缓冲channel

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
				break
			}
		}
		// 如果该key没有订阅者了，删除key
		if len(m.channels[key]) == 0 {
			delete(m.channels, key)
		}
	}
	close(ch)
}

// Unsubscribe 主动取消订阅（可选方法）
func (m *MemorySubscriber) Unsubscribe(key string, ch <-chan string) {
	key = m.prefix + key
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果已关闭，不需要清理
	if m.closed {
		return
	}

	if channels, exists := m.channels[key]; exists {
		for i, c := range channels {
			if c == ch {
				m.channels[key] = append(channels[:i], channels[i+1:]...)
				close(c) // 关闭channel
				break
			}
		}
		if len(m.channels[key]) == 0 {
			delete(m.channels, key)
		}
	}
}
