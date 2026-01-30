package subscribe

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/client/v3"
)

// EtcdSubscriber 是基于etcd的订阅者实现
// EtcdSubscriber is an etcd-based subscriber implementation.
type EtcdSubscriber struct {
	prefix   string
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers map[string][]*etcdWatcher // active watchers, supporting multiple watchers per key
	closed   bool
}

// etcdWatcher represents a single subscription watcher.
type etcdWatcher struct {
	cancel context.CancelFunc
	ch     chan string
	id     string // unique identifier for the watcher
}

// NewEtcdSubscriber 创建etcd订阅者
// NewEtcdSubscriber creates a new etcd subscriber.
func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   strings.TrimSuffix(prefix, "/"),
		client:   client,
		watchers: make(map[string][]*etcdWatcher),
		closed:   false,
	}
}

// Child 创建子订阅者
// Child creates a child subscriber with appended path.
func (e *EtcdSubscriber) Child(paths ...string) Subscriber {
	if len(paths) == 0 {
		return e
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
		return e
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Ensure slash separator
	basePrefix := e.prefix
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	newPrefix := basePrefix + strings.Join(keys, "/") + "/"

	if e.closed {
		child := &EtcdSubscriber{
			prefix:   newPrefix,
			client:   e.client,
			watchers: make(map[string][]*etcdWatcher),
			closed:   true,
		}
		return child
	}

	return &EtcdSubscriber{
		prefix:   newPrefix,
		client:   e.client,
		watchers: make(map[string][]*etcdWatcher),
		closed:   false,
	}
}

// Publish 发布消息到etcd
// Publish publishes a message to the specified key in etcd.
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

// Subscribe subscribes to updates for the given key.
// Returns a channel that receives value updates.
func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		ch := make(chan string, 100)
		close(ch)
		return ch, nil
	}

	fullKey := e.buildFullKey(key)

	// Create a buffered channel for the watcher
	watchCh := make(chan string, 1000)
	// Use the passed context as parent to allow external cancellation
	watchCtx, cancel := context.WithCancel(ctx)

	watcher := &etcdWatcher{
		cancel: cancel,
		ch:     watchCh,
		id:     generateWatcherID(),
	}

	// Append to the list of watchers for this key
	e.watchers[fullKey] = append(e.watchers[fullKey], watcher)

	// Start the watcher goroutine
	go e.watchKey(watchCtx, fullKey, watchCh, watcher.id)

	return watchCh, nil
}

// watchKey monitors a single etcd key for updates.
func (e *EtcdSubscriber) watchKey(ctx context.Context, key string, outputCh chan<- string, watcherID string) {
	// Ensure the output channel is closed when the watcher exits
	defer close(outputCh)
	// Ensure the watcher is removed from the map when it exits
	defer e.cleanupWatcher(key, watcherID)

	watchChan := e.client.Watch(ctx, key)

	for {
		select {
		case <-ctx.Done():
			return
		case watchResp, ok := <-watchChan:
			if !ok {
				return
			}
			if err := watchResp.Err(); err != nil {
				continue
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypePut {
					select {
					case outputCh <- string(event.Kv.Value):
						// Successfully sent
					case <-ctx.Done():
						return
					default:
						// Drop message if channel is full
					}
				}
			}
		}
	}
}

// cleanupWatcher removes the watcher from the internal map.
func (e *EtcdSubscriber) cleanupWatcher(key, watcherID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if watchers, exists := e.watchers[key]; exists {
		for i, watcher := range watchers {
			if watcher.id == watcherID {
				// Remove from slice
				e.watchers[key] = append(watchers[:i], watchers[i+1:]...)
				// Channel is closed by watchKey, so we don't need to close it here
				// If no more watchers for this key, remove the key entry
				if len(e.watchers[key]) == 0 {
					delete(e.watchers, key)
				}
				break
			}
		}
	}
}

// buildFullKey constructs the full key path.
func (e *EtcdSubscriber) buildFullKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	if e.prefix == "" {
		return key
	}
	if strings.HasSuffix(e.prefix, "/") {
		return e.prefix + key
	}
	return e.prefix + "/" + key
}

// Close 关闭etcd订阅者
// Close closes the subscriber and cancels all active watchers.
func (e *EtcdSubscriber) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true

	// Cancel all watchers
	for key, watchers := range e.watchers {
		for _, watcher := range watchers {
			watcher.cancel()
		}
		delete(e.watchers, key)
	}

	return nil
}

// Helper variables for generating unique watcher IDs
var (
	watcherCounter uint64
	watcherMutex   sync.Mutex
)

func generateWatcherID() string {
	watcherMutex.Lock()
	defer watcherMutex.Unlock()
	watcherCounter++
	return strconv.FormatUint(watcherCounter, 10)
}
