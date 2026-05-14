package subscribe

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdSubscriber struct {
	prefix   string
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers map[string][]*etcdSubscription
	closed   bool
	parent   *EtcdSubscriber
	children []*EtcdSubscriber
}

type etcdSubscription struct {
	key    string
	events chan Event
	errors chan error
	cancel context.CancelFunc
	id     string
	owner  *EtcdSubscriber
	once   sync.Once
}

func (s *etcdSubscription) Events() <-chan Event { return s.events }

func (s *etcdSubscription) Errors() <-chan error { return s.errors }

func (s *etcdSubscription) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	s.once.Do(func() {
		s.cancel()
		s.owner.cleanupWatcher(s.key, s.id)
		close(s.events)
		close(s.errors)
	})
	return nil
}

func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   strings.TrimSuffix(prefix, "/"),
		client:   client,
		watchers: make(map[string][]*etcdSubscription),
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
	e.mu.Lock()
	defer e.mu.Unlock()

	basePrefix := e.prefix
	if basePrefix != "" && !strings.HasSuffix(basePrefix, "/") {
		basePrefix += "/"
	}
	child := &EtcdSubscriber{
		prefix:   basePrefix + strings.Join(keys, "/") + "/",
		client:   e.client,
		watchers: make(map[string][]*etcdSubscription),
		parent:   e,
	}
	e.children = append(e.children, child)
	return child
}

func (e *EtcdSubscriber) Publish(ctx context.Context, key, data string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.isClosedLocked() {
		return nil
	}

	fullKey := e.buildFullKey(key)
	_, err := e.client.Put(ctx, fullKey, data)
	return err
}

func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (Subscription, error) {
	e.mu.Lock()
	fullKey := e.buildFullKey(key)
	events := make(chan Event, 1000)
	errorsCh := make(chan error, 32)
	watchCtx, cancel := context.WithCancel(ctx)
	sub := &etcdSubscription{
		key:    fullKey,
		events: events,
		errors: errorsCh,
		cancel: cancel,
		id:     generateWatcherID(),
		owner:  e,
	}

	if e.isClosedLocked() {
		e.mu.Unlock()
		close(events)
		close(errorsCh)
		return sub, nil
	}
	e.mu.Unlock()

	// Capture a revision barrier before exposing the subscription to callers.
	// The watcher starts from revision+1 so publishes after Subscribe returns
	// are not lost even if the watch stream is established slightly later.
	resp, err := e.client.Get(watchCtx, fullKey)
	if err != nil {
		cancel()
		close(events)
		close(errorsCh)
		return nil, err
	}
	startRevision := resp.Header.Revision

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosedLocked() {
		cancel()
		close(events)
		close(errorsCh)
		return sub, nil
	}
	e.watchers[fullKey] = append(e.watchers[fullKey], sub)
	go e.watchKey(watchCtx, fullKey, startRevision, sub)
	return sub, nil
}

func (e *EtcdSubscriber) watchKey(ctx context.Context, key string, startRevision int64, sub *etcdSubscription) {
	lastRevision := startRevision
	backoff := 50 * time.Millisecond

	for {
		if ctx.Err() != nil {
			_ = sub.Close()
			return
		}

		opts := []clientv3.OpOption{}
		if lastRevision > 0 {
			opts = append(opts, clientv3.WithRev(lastRevision+1))
		}
		watchChan := e.client.Watch(ctx, key, opts...)

		restart := false
		for !restart {
			select {
			case <-ctx.Done():
				_ = sub.Close()
				return
			case watchResp, ok := <-watchChan:
				if !ok {
					restart = true
					break
				}
				if watchResp.Header.Revision > lastRevision {
					lastRevision = watchResp.Header.Revision
				}
				if err := watchResp.Err(); err != nil {
					select {
					case sub.errors <- err:
					default:
					}
					restart = true
					break
				}
				for _, event := range watchResp.Events {
					if event.Type != clientv3.EventTypePut {
						continue
					}
					payload := Event{
						Key:      string(event.Kv.Key),
						Value:    string(event.Kv.Value),
						Revision: event.Kv.ModRevision,
					}
					select {
					case sub.events <- payload:
					case <-ctx.Done():
						_ = sub.Close()
						return
					default:
					}
				}
			}
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = sub.Close()
			return
		case <-timer.C:
		}
		if backoff < time.Second {
			backoff *= 2
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
	e.closeLocked()
	return nil
}

func (e *EtcdSubscriber) closeLocked() {
	if e.closed {
		return
	}
	e.closed = true
	for _, child := range e.children {
		child.closeLocked()
	}
	for key, watchers := range e.watchers {
		for _, watcher := range watchers {
			watcher.once.Do(func() {
				watcher.cancel()
				close(watcher.events)
				close(watcher.errors)
			})
		}
		delete(e.watchers, key)
	}
}

func (e *EtcdSubscriber) isClosedLocked() bool {
	for current := e; current != nil; current = current.parent {
		if current.closed {
			return true
		}
	}
	return false
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
