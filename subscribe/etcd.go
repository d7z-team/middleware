package subscribe

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"go.etcd.io/etcd/client/v3"
)

type EtcdSubscriber struct {
	prefix   string
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers map[string][]*etcdWatcher
	closed   bool
}

type etcdWatcher struct {
	cancel context.CancelFunc
	ch     chan string
	id     string
}

// NewEtcdSubscriber creates an etcd-backed subscriber.
//
// Example:
//
//	sub := NewEtcdSubscriber(client, "events")
func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   strings.TrimSuffix(prefix, "/"),
		client:   client,
		watchers: make(map[string][]*etcdWatcher),
		closed:   false,
	}
}

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

func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (<-chan string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		ch := make(chan string, 100)
		close(ch)
		return ch, nil
	}

	fullKey := e.buildFullKey(key)

	watchCh := make(chan string, 1000)
	watchCtx, cancel := context.WithCancel(ctx)

	watcher := &etcdWatcher{
		cancel: cancel,
		ch:     watchCh,
		id:     generateWatcherID(),
	}

	e.watchers[fullKey] = append(e.watchers[fullKey], watcher)

	go e.watchKey(watchCtx, fullKey, watchCh, watcher.id)

	return watchCh, nil
}

func (e *EtcdSubscriber) watchKey(ctx context.Context, key string, outputCh chan<- string, watcherID string) {
	defer close(outputCh)
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
					case <-ctx.Done():
						return
					default:
					}
				}
			}
		}
	}
}

func (e *EtcdSubscriber) cleanupWatcher(key, watcherID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if watchers, exists := e.watchers[key]; exists {
		for i, watcher := range watchers {
			if watcher.id == watcherID {
				e.watchers[key] = append(watchers[:i], watchers[i+1:]...)
				if len(e.watchers[key]) == 0 {
					delete(e.watchers, key)
				}
				break
			}
		}
	}
}

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

func (e *EtcdSubscriber) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true

	for key, watchers := range e.watchers {
		for _, watcher := range watchers {
			watcher.cancel()
		}
		delete(e.watchers, key)
	}

	return nil
}

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
