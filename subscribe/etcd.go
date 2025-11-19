package subscribe

import (
	"context"
	"strings"
	"sync"

	"go.etcd.io/etcd/client/v3"
)

// EtcdSubscriber etcd版本的订阅者实现
type EtcdSubscriber struct {
	prefix   string
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers map[string]*etcdWatcher
}

type etcdWatcher struct {
	cancel context.CancelFunc
	ch     chan string
}

// NewEtcdSubscriber 创建etcd订阅者
func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   strings.TrimSuffix(prefix, "/"),
		client:   client,
		watchers: make(map[string]*etcdWatcher),
	}
}

// Child 创建子订阅者
func (e *EtcdSubscriber) Child(prefix string) Subscriber {
	return &EtcdSubscriber{
		prefix:   e.buildFullKey(prefix),
		client:   e.client,
		watchers: e.watchers,
	}
}

// Publish 发布消息到etcd
func (e *EtcdSubscriber) Publish(ctx context.Context, key string, data string) error {
	fullKey := e.buildFullKey(key)
	_, err := e.client.Put(ctx, fullKey, data)
	return err
}

// Subscribe 订阅etcd key的变化
func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	fullKey := e.buildFullKey(key)

	// 如果已经有watcher，直接返回
	if watcher, exists := e.watchers[fullKey]; exists {
		return watcher.ch, nil
	}

	// 创建新的watcher
	watchCh := make(chan string, 100)
	watchCtx, cancel := context.WithCancel(context.Background())

	watcher := &etcdWatcher{
		cancel: cancel,
		ch:     watchCh,
	}
	e.watchers[fullKey] = watcher

	// 启动etcd watch
	go e.watchKey(watchCtx, fullKey, watchCh)

	// 监听上下文取消
	go func() {
		<-ctx.Done()
		e.unsubscribe(fullKey)
	}()

	return watchCh, nil
}

// watchKey 监听单个etcd key的变化
func (e *EtcdSubscriber) watchKey(ctx context.Context, key string, outputCh chan<- string) {
	watchChan := e.client.Watch(ctx, key)

	for {
		select {
		case <-ctx.Done():
			return
		case watchResp := <-watchChan:
			if err := watchResp.Err(); err != nil {
				continue
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					select {
					case outputCh <- string(event.Kv.Value):
					case <-ctx.Done():
						return
					default:
						// channel已满，丢弃消息
					}
				}
			}
		}
	}
}

// Unsubscribe 取消订阅
func (e *EtcdSubscriber) Unsubscribe(key string) {
	fullKey := e.buildFullKey(key)
	e.unsubscribe(fullKey)
}

// unsubscribe 内部取消订阅实现
func (e *EtcdSubscriber) unsubscribe(fullKey string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if watcher, exists := e.watchers[fullKey]; exists {
		watcher.cancel()
		close(watcher.ch)
		delete(e.watchers, fullKey)
	}
}

// buildFullKey 构建完整的key
func (e *EtcdSubscriber) buildFullKey(key string) string {
	if e.prefix == "" {
		return key
	}
	return e.prefix + "/" + strings.TrimPrefix(key, "/")
}

// Close 关闭etcd客户端
func (e *EtcdSubscriber) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, watcher := range e.watchers {
		watcher.cancel()
		close(watcher.ch)
	}
	e.watchers = make(map[string]*etcdWatcher)

	return e.client.Close()
}
