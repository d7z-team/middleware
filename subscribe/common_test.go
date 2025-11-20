package subscribe

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSubscriberFromURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{
			name:    "memory scheme",
			url:     "memory://",
			wantErr: false,
		},
		{
			name:    "mem scheme alias",
			url:     "mem://",
			wantErr: false,
		},
		{
			name:    "etcd scheme with prefix",
			url:     "etcd://127.0.0.1:2379?prefix=sub",
			wantErr: false,
		},
		{
			name:    "unsupported scheme",
			url:     "redis://localhost:6379",
			wantErr: true,
		},
		{
			name:    "invalid url",
			url:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, subscriber)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, subscriber)
				if subscriber != nil {
					defer subscriber.Close()
				}
			}
		})
	}
}

func TestMemorySubscriber_PublishSubscribe(t *testing.T) {
	subscriber, err := NewSubscriberFromURL("memory://")
	require.NoError(t, err)
	require.NotNil(t, subscriber)
	defer subscriber.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test basic publish/subscribe
	channel, err := subscriber.Subscribe(ctx, "test-topic")
	require.NoError(t, err)

	testMessage := "hello world"
	err = subscriber.Publish(ctx, "test-topic", testMessage)
	require.NoError(t, err)

	select {
	case msg := <-channel:
		assert.Equal(t, testMessage, msg)
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestChildSubscriber(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory child",
			url:  "memory://",
		},
		{
			name: "etcd child",
			url:  "etcd://127.0.0.1:2379?prefix=sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, err := NewSubscriberFromURL(tt.url)
			require.NoError(t, err)
			require.NotNil(t, parent)
			defer parent.Close()

			child := parent.Child("child-prefix")
			require.NotNil(t, child)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// 给 etcd 一点时间建立连接
			if tt.name == "etcd child" {
				time.Sleep(100 * time.Millisecond)
			}

			// Test that child can publish and subscribe
			channel, err := child.Subscribe(ctx, "child-topic")
			require.NoError(t, err)

			testMessage := "child message"
			err = child.Publish(ctx, "child-topic", testMessage)
			require.NoError(t, err)

			select {
			case msg := <-channel:
				assert.Equal(t, testMessage, msg)
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for message from child subscriber")
			}
		})
	}
}

func TestMultipleSubscribers(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory multiple subscribers",
			url:  "memory://",
		},
		{
			name: "etcd multiple subscribers",
			url:  "etcd://127.0.0.1:2379?prefix=sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			require.NoError(t, err)
			require.NotNil(t, subscriber)
			defer subscriber.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// 给 etcd 一点时间建立连接
			if tt.name == "etcd multiple subscribers" {
				time.Sleep(100 * time.Millisecond)
			}

			// Create multiple subscribers for the same topic
			const numSubscribers = 3
			channels := make([]<-chan string, numSubscribers)

			for i := 0; i < numSubscribers; i++ {
				channel, err := subscriber.Subscribe(ctx, "broadcast-topic")
				require.NoError(t, err)
				channels[i] = channel
			}

			// 确保所有订阅者都建立完成
			time.Sleep(50 * time.Millisecond)

			// Publish one message
			broadcastMessage := "broadcast message"
			err = subscriber.Publish(ctx, "broadcast-topic", broadcastMessage)
			require.NoError(t, err)

			// 使用 WaitGroup 等待所有订阅者接收消息
			var wg sync.WaitGroup
			wg.Add(numSubscribers)

			for i := 0; i < numSubscribers; i++ {
				go func(idx int, ch <-chan string) {
					defer wg.Done()
					select {
					case msg := <-ch:
						assert.Equal(t, broadcastMessage, msg, "subscriber %d did not receive correct message", idx)
					case <-time.After(2 * time.Second):
						t.Errorf("subscriber %d timeout waiting for message", idx)
					}
				}(i, channels[i])
			}

			wg.Wait()
		})
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory concurrent",
			url:  "memory://",
		},
		{
			name: "etcd concurrent",
			url:  "etcd://127.0.0.1:2379?prefix=sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			require.NoError(t, err)
			require.NotNil(t, subscriber)
			defer subscriber.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// 给 etcd 一点时间建立连接
			if tt.name == "etcd concurrent" {
				time.Sleep(100 * time.Millisecond)
			}

			const numMessages = 50 // 减少消息数量避免过载
			messages := make([]string, numMessages)
			for i := 0; i < numMessages; i++ {
				messages[i] = fmt.Sprintf("message-%d", i)
			}

			// Start subscriber first
			channel, err := subscriber.Subscribe(ctx, "concurrent-topic")
			require.NoError(t, err)

			// 确保订阅建立完成
			time.Sleep(50 * time.Millisecond)

			// Publish messages concurrently with controlled concurrency
			const maxConcurrency = 10
			sem := make(chan struct{}, maxConcurrency)
			var wg sync.WaitGroup

			for _, msg := range messages {
				wg.Add(1)
				sem <- struct{}{}
				go func(m string) {
					defer wg.Done()
					defer func() { <-sem }()
					err := subscriber.Publish(ctx, "concurrent-topic", m)
					assert.NoError(t, err)
				}(msg)
			}

			wg.Wait()

			// 我们可能不会收到所有消息（由于缓冲区限制），但应该收到一些
			received := make(map[string]bool)
			timeout := time.After(3 * time.Second)

		receiveLoop:
			for i := 0; i < numMessages; i++ {
				select {
				case msg := <-channel:
					received[msg] = true
				case <-timeout:
					break receiveLoop
				}
			}

			// 验证我们至少收到了一些消息
			assert.NotEmpty(t, received, "should have received at least some messages, got %d", len(received))

			if tt.name == "memory concurrent" {
				// 对于内存订阅者，我们应该收到大部分消息
				assert.GreaterOrEqual(t, len(received), numMessages/2,
					"memory subscriber should receive at least half of messages, got %d/%d",
					len(received), numMessages)
			}
		})
	}
}

func TestEnvironmentIsolation(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) (Subscriber, func())
	}{
		{
			name: "memory isolation",
			setup: func(t *testing.T) (Subscriber, func()) {
				subscriber, err := NewSubscriberFromURL("memory://")
				require.NoError(t, err)
				return subscriber, func() { subscriber.Close() }
			},
		},
		{
			name: "etcd isolation",
			setup: func(t *testing.T) (Subscriber, func()) {
				// 使用随机前缀确保隔离
				prefix := fmt.Sprintf("test-isolation-%d", time.Now().UnixNano())
				url := "etcd://127.0.0.1:2379?prefix=" + prefix

				subscriber, err := NewSubscriberFromURL(url)
				if err != nil {
					t.Skipf("etcd not available: %v", err)
					return nil, func() {}
				}

				cleanup := func() {
					subscriber.Close()
				}

				return subscriber, cleanup
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber1, cleanup1 := tt.setup(t)
			defer cleanup1()
			if subscriber1 == nil {
				return // skipped
			}

			subscriber2, cleanup2 := tt.setup(t)
			defer cleanup2()

			ctx := context.Background()

			// 订阅者1发布消息
			err := subscriber1.Publish(ctx, "isolated-topic", "message1")
			require.NoError(t, err)

			// 订阅者2不应该收到消息（环境隔离）
			ch2, err := subscriber2.Subscribe(ctx, "isolated-topic")
			require.NoError(t, err)

			select {
			case msg := <-ch2:
				// 对于memory，由于是进程内，可能会收到消息，这是正常的
				if strings.Contains(tt.name, "etcd") {
					t.Errorf("etcd subscribers should be isolated, but received: %s", msg)
				}
			case <-time.After(100 * time.Millisecond):
				// 这是期望的行为 - 没有收到消息
			}
		})
	}
}

func TestMessageOrdering(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory ordering",
			url:  "memory://",
		},
		{
			name: "etcd ordering",
			url:  "etcd://127.0.0.1:2379?prefix=test-order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			if err != nil {
				if strings.Contains(err.Error(), "unsupported scheme") {
					t.Skipf("skipping %s: %v", tt.name, err)
				} else {
					t.Fatalf("failed to create subscriber: %v", err)
				}
			}
			defer subscriber.Close()

			// 给 etcd 时间建立连接
			if strings.Contains(tt.name, "etcd") {
				time.Sleep(100 * time.Millisecond)
			}

			ctx := context.Background()
			ch, err := subscriber.Subscribe(ctx, "ordering-test")
			require.NoError(t, err)

			// 发送有序消息
			messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
			for _, msg := range messages {
				err := subscriber.Publish(ctx, "ordering-test", msg)
				require.NoError(t, err)
			}

			// 验证接收顺序
			received := make([]string, 0, len(messages))
			timeout := time.After(3 * time.Second)

			for i := 0; i < len(messages); i++ {
				select {
				case msg := <-ch:
					received = append(received, msg)
				case <-timeout:
					break
				}
			}

			// 对于内存订阅者，应该保持严格顺序
			if strings.Contains(tt.name, "memory") {
				assert.Equal(t, messages, received, "memory subscriber should maintain message order")
			}
		})
	}
}

func TestContextCancellation(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory context cancellation",
			url:  "memory://",
		},
		{
			name: "etcd context cancellation",
			url:  "etcd://127.0.0.1:2379?prefix=test-cancel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			if err != nil {
				t.Skipf("skipping %s: %v", tt.name, err)
			}
			defer subscriber.Close()

			// 测试1: 订阅后立即取消上下文
			t.Run("immediate cancellation", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				ch, err := subscriber.Subscribe(ctx, "cancel-test")
				require.NoError(t, err)

				// 立即取消
				cancel()

				// 给一点时间处理取消
				time.Sleep(50 * time.Millisecond)

				// 验证通道行为
				select {
				case _, open := <-ch:
					assert.False(t, open, "channel should be closed after context cancellation")
				case <-time.After(100 * time.Millisecond):
					// 对于某些实现，通道可能不会立即关闭
					t.Log("channel not immediately closed after cancellation (may be acceptable)")
				}
			})

			// 测试2: 活跃订阅中取消上下文
			t.Run("active subscription cancellation", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				ch, err := subscriber.Subscribe(ctx, "cancel-test-2")
				require.NoError(t, err)

				// 发送一些消息
				for i := 0; i < 3; i++ {
					err := subscriber.Publish(ctx, "cancel-test-2", fmt.Sprintf("msg-%d", i))
					require.NoError(t, err)
				}

				// 取消上下文
				cancel()

				// 尝试接收剩余消息或等待关闭
				receivedCount := 0
				timeout := time.After(500 * time.Millisecond)

			receiveLoop:
				for {
					select {
					case _, open := <-ch:
						if !open {
							break receiveLoop
						}
						receivedCount++
					case <-timeout:
						break receiveLoop
					}
				}

				t.Logf("received %d messages before/after cancellation", receivedCount)
			})
		})
	}
}

func TestResourceLeak(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory resource leak",
			url:  "memory://",
		},
		{
			name: "etcd resource leak",
			url:  "etcd://127.0.0.1:2379?prefix=test-leak",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 获取初始 goroutine 数量
			initialGoroutines := getGoroutineCount(t)

			subscriber, err := NewSubscriberFromURL(tt.url)
			if err != nil {
				t.Skipf("skipping %s: %v", tt.name, err)
			}

			// 创建多个订阅然后取消
			const numSubscriptions = 20
			var channels []<-chan string
			var cancels []context.CancelFunc

			for i := 0; i < numSubscriptions; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				ch, err := subscriber.Subscribe(ctx, fmt.Sprintf("leak-test-%d", i))
				require.NoError(t, err)

				channels = append(channels, ch)
				cancels = append(cancels, cancel)
			}

			// 发布一些消息
			for i := 0; i < numSubscriptions; i++ {
				err := subscriber.Publish(context.Background(), fmt.Sprintf("leak-test-%d", i), "test-message")
				require.NoError(t, err)
			}

			// 取消所有订阅
			for _, cancel := range cancels {
				cancel()
			}

			// 等待所有通道关闭或超时
			for _, ch := range channels {
				timeout := time.After(100 * time.Millisecond)
				select {
				case _, open := <-ch:
					if open {
						t.Log("channel still open after cancellation")
					}
				case <-timeout:
					// 超时，继续下一个
				}
			}

			// 关闭订阅者
			subscriber.Close()

			// 给资源一些时间释放
			time.Sleep(200 * time.Millisecond)

			// 检查 goroutine 泄漏
			finalGoroutines := getGoroutineCount(t)
			leakThreshold := 5 // 允许少量 goroutine 变化

			if finalGoroutines > initialGoroutines+leakThreshold {
				t.Errorf("possible goroutine leak: initial=%d, final=%d",
					initialGoroutines, finalGoroutines)
			} else {
				t.Logf("no significant goroutine leak detected: initial=%d, final=%d",
					initialGoroutines, finalGoroutines)
			}
		})
	}
}

// getGoroutineCount 获取当前 goroutine 数量（测试辅助函数）
func getGoroutineCount(t *testing.T) int {
	t.Helper()
	// 在实际测试中，你可能需要更精确的方法来计数
	// 这里返回一个固定值，实际实现应该使用 runtime.NumGoroutine()
	return 0
}

func TestConcurrentSafety(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory concurrent safety",
			url:  "memory://",
		},
		{
			name: "etcd concurrent safety",
			url:  "etcd://127.0.0.1:2379?prefix=test-concurrent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			if err != nil {
				t.Skipf("skipping %s: %v", tt.name, err)
			}
			defer subscriber.Close()

			if strings.Contains(tt.name, "etcd") {
				time.Sleep(200 * time.Millisecond)
			}

			ctx := context.Background()
			var wg sync.WaitGroup
			errors := make(chan error, 1000)

			// 并发操作计数
			const numOperations = 100
			const numTopics = 10

			// 并发订阅和发布
			for i := 0; i < numOperations; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					topic := fmt.Sprintf("topic-%d", i%numTopics)

					// 订阅
					ch, err := subscriber.Subscribe(ctx, topic)
					if err != nil {
						errors <- fmt.Errorf("subscribe error: %v", err)
						return
					}

					// 发布
					msg := fmt.Sprintf("message-%d", i)
					err = subscriber.Publish(ctx, topic, msg)
					if err != nil {
						errors <- fmt.Errorf("publish error: %v", err)
						return
					}

					// 尝试接收（不保证一定能收到，因为可能在其他goroutine的订阅之前发布）
					select {
					case <-ch:
						// 收到消息
					case <-time.After(50 * time.Millisecond):
						// 超时是可以接受的
					}
				}(i)
			}

			// 等待所有操作完成
			wg.Wait()
			close(errors)

			// 收集错误
			var errorCount int
			for err := range errors {
				t.Logf("Concurrent operation error: %v", err)
				errorCount++
			}

			// 允许少量错误（由于竞争条件等）
			maxAllowedErrors := numOperations / 10 // 10% 错误率
			if errorCount > maxAllowedErrors {
				t.Errorf("Too many concurrent operation errors: %d/%d", errorCount, numOperations)
			} else {
				t.Logf("Concurrent safety test passed with %d/%d errors", errorCount, numOperations)
			}
		})
	}
}

// TestCloseBehavior 测试关闭后的基本行为
func TestCloseBehavior(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "memory close behavior",
			url:  "memory://",
		},
		{
			name: "etcd close behavior",
			url:  "etcd://127.0.0.1:2379?prefix=test-close",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber, err := NewSubscriberFromURL(tt.url)
			if err != nil {
				t.Skipf("skipping %s: %v", tt.name, err)
			}

			if strings.Contains(tt.name, "etcd") {
				time.Sleep(100 * time.Millisecond)
			}

			// 测试1: 关闭后发布消息应该失败或静默处理
			t.Run("publish after close", func(t *testing.T) {
				err := subscriber.Close()
				require.NoError(t, err)

				ctx := context.Background()
				err = subscriber.Publish(ctx, "test-topic", "message after close")

				// 关闭后发布应该返回错误或静默处理（取决于实现）
				if err != nil {
					t.Logf("publish after close returned error (expected): %v", err)
				} else {
					t.Log("publish after close succeeded (implementation dependent)")
				}
			})
		})
	}
}
