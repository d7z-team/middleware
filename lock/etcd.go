package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type EtcdLocker struct {
	client *clientv3.Client
	prefix string
	closer func() error
}

// NewEtcdLocker creates an etcd-backed locker.
//
// Example:
//
//	locker := NewEtcdLocker(client, "/locks/")
func NewEtcdLocker(client *clientv3.Client, prefix string) *EtcdLocker {
	return newEtcdLocker(client, prefix, func() error { return nil })
}

func newEtcdLocker(client *clientv3.Client, prefix string, closer func() error) *EtcdLocker {
	if prefix == "" {
		prefix = "/locks/"
	} else if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	return &EtcdLocker{
		client: client,
		prefix: prefix,
		closer: closer,
	}
}

func (e *EtcdLocker) Close() error {
	if e.closer == nil {
		return nil
	}
	return e.closer()
}

func (e *EtcdLocker) getLockPath(id string) string {
	return fmt.Sprintf("%s%s", e.prefix, id)
}

func (e *EtcdLocker) TryLock(ctx context.Context, id string) func() {
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil
	}

	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	if err := mutex.TryLock(ctx); err != nil {
		_ = session.Close()
		return nil
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_ = mutex.Unlock(cleanupCtx)
			_ = session.Close()
		})
	}
}

func (e *EtcdLocker) Lock(ctx context.Context, id string) func() {
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil
	}

	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_ = mutex.Unlock(cleanupCtx)
			_ = session.Close()
		})
	}
}
