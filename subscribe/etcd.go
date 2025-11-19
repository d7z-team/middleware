package subscribe

import (
	"context"
	"strings"
	"sync"

	"go.etcd.io/etcd/client/v3"
)

// EtcdSubscriber 修复后的etcd订阅者实现
type EtcdSubscriber struct {
	prefix   string
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers map[string][]*etcdWatcher // 改为切片，支持多个watcher
	closed   bool
}

type etcdWatcher struct {
	cancel context.CancelFunc
	ch     chan string
	id     string // 唯一标识符
}

// NewEtcdSubscriber 创建etcd订阅者
func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   strings.TrimSuffix(prefix, "/"),
		client:   client,
		watchers: make(map[string][]*etcdWatcher),
		closed:   false,
	}
}

// Child 创建子订阅者
func (e *EtcdSubscriber) Child(prefix string) Subscriber {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		child := &EtcdSubscriber{
			prefix:   e.buildFullKey(prefix),
			client:   e.client,
			watchers: make(map[string][]*etcdWatcher),
			closed:   true,
		}
		return child
	}

	return &EtcdSubscriber{
		prefix:   e.buildFullKey(prefix),
		client:   e.client,
		watchers: make(map[string][]*etcdWatcher),
		closed:   false,
	}
}

// Publish 发布消息到etcd
func (e *EtcdSubscriber) Publish(ctx context.Context, key, data string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil
	}

	fullKey := e.buildFullKey(key)
	_, err := e.client.Put(ctx, fullKey, data)
	return err
}

// Subscribe 订阅etcd key的变化 - 修复：每个订阅者创建独立的watcher
func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		ch := make(chan string, 100)
		close(ch)
		return ch, nil
	}

	fullKey := e.buildFullKey(key)

	// 为每个订阅创建独立的watcher
	watchCh := make(chan string, 1000)
	watchCtx, cancel := context.WithCancel(context.Background())

	watcher := &etcdWatcher{
		cancel: cancel,
		ch:     watchCh,
		id:     generateWatcherID(), // 生成唯一ID
	}

	// 添加到watchers列表
	e.watchers[fullKey] = append(e.watchers[fullKey], watcher)

	// 启动etcd watch
	go e.watchKey(watchCtx, fullKey, watchCh, watcher.id)

	return watchCh, nil
}

// watchKey 监听单个etcd key的变化 - 修复：支持多个watcher
func (e *EtcdSubscriber) watchKey(ctx context.Context, key string, outputCh chan<- string, watcherID string) {
	watchChan := e.client.Watch(ctx, key)

	for {
		select {
		case <-ctx.Done():
			// watcher被取消，清理资源
			e.cleanupWatcher(key, watcherID)
			return
		case watchResp, ok := <-watchChan:
			if !ok {
				e.cleanupWatcher(key, watcherID)
				return
			}
			if err := watchResp.Err(); err != nil {
				continue
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					select {
					case outputCh <- string(event.Kv.Value):
						// 成功发送消息
					case <-ctx.Done():
						e.cleanupWatcher(key, watcherID)
						return
					default:
						// channel已满，丢弃消息
					}
				}
			}
		}
	}
}

// cleanupWatcher 清理watcher资源
func (e *EtcdSubscriber) cleanupWatcher(key, watcherID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if watchers, exists := e.watchers[key]; exists {
		for i, watcher := range watchers {
			if watcher.id == watcherID {
				// 从切片中移除
				e.watchers[key] = append(watchers[:i], watchers[i+1:]...)
				close(watcher.ch)
				// 如果该key没有watcher了，删除key
				if len(e.watchers[key]) == 0 {
					delete(e.watchers, key)
				}
				break
			}
		}
	}
}

// buildFullKey 构建完整的key
func (e *EtcdSubscriber) buildFullKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if e.prefix == "" {
		return key
	}
	return e.prefix + "/" + key
}

// Close 关闭etcd订阅者
func (e *EtcdSubscriber) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true

	// 关闭所有watchers
	for key, watchers := range e.watchers {
		for _, watcher := range watchers {
			watcher.cancel()
			close(watcher.ch)
		}
		delete(e.watchers, key)
	}

	return nil
}

// 生成唯一watcher ID的辅助函数
var (
	watcherCounter uint64
	watcherMutex   sync.Mutex
)

func generateWatcherID() string {
	watcherMutex.Lock()
	defer watcherMutex.Unlock()
	watcherCounter++
	return string(rune(watcherCounter))
}
