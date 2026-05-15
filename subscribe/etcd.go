package subscribe

import (
	"context"
	"strconv"
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
	state  *subscriptionState
	cancel context.CancelFunc
	id     string
	owner  *EtcdSubscriber
	once   sync.Once
}

func (s *etcdSubscription) Events() <-chan Event { return s.state.Events() }

func (s *etcdSubscription) Errors() <-chan error { return s.state.Errors() }

func (s *etcdSubscription) Close() error {
	if s == nil || s.owner == nil {
		return nil
	}
	s.once.Do(func() {
		s.cancel()
		s.state.closeChannels()
	})
	s.owner.cleanupWatcher(s.key, s.id)
	return nil
}

func NewEtcdSubscriber(client *clientv3.Client, prefix string) *EtcdSubscriber {
	return &EtcdSubscriber{
		prefix:   subscriberChildPrefix("", prefix),
		client:   client,
		watchers: make(map[string][]*etcdSubscription),
	}
}

func (e *EtcdSubscriber) Child(paths ...string) Subscriber {
	childPrefix := subscriberChildPrefix(e.prefix, paths...)
	if childPrefix == e.prefix {
		return e
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	child := &EtcdSubscriber{
		prefix:   childPrefix,
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

	fullKey := buildSubscriberKey(e.prefix, key)
	_, err := e.client.Put(ctx, fullKey, data)
	return err
}

func (e *EtcdSubscriber) Subscribe(ctx context.Context, key string) (Subscription, error) {
	e.mu.Lock()
	fullKey := buildSubscriberKey(e.prefix, key)
	watchCtx, cancel := context.WithCancel(ctx)
	sub := &etcdSubscription{
		key:    fullKey,
		state:  newSubscriptionState(),
		cancel: cancel,
		id:     generateWatcherID(),
		owner:  e,
	}

	if e.isClosedLocked() {
		e.mu.Unlock()
		sub.cancel()
		sub.state.closeChannels()
		return sub, nil
	}
	e.mu.Unlock()

	// Capture a revision barrier before exposing the subscription to callers.
	// The watcher starts from revision+1 so publishes after Subscribe returns
	// are not lost even if the watch stream is established slightly later.
	resp, err := e.client.Get(watchCtx, fullKey)
	if err != nil {
		cancel()
		sub.state.closeChannels()
		return nil, err
	}
	startRevision := resp.Header.Revision

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosedLocked() {
		cancel()
		sub.state.closeChannels()
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
					sub.state.trySendError(err)
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
					if err := sub.state.trySendEvent(ctx, payload); err != nil {
						_ = sub.Close()
						return
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
			watcher := watcher
			watcher.once.Do(func() {
				watcher.cancel()
				watcher.state.closeChannels()
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
