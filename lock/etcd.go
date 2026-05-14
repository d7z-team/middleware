package lock

import (
	"context"
	"errors"
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

type etcdLockHandle struct {
	mutex   *concurrency.Mutex
	session *concurrency.Session
	once    sync.Once
}

func (h *etcdLockHandle) Unlock() error {
	if h == nil || h.mutex == nil || h.session == nil {
		return nil
	}
	var unlockErr error
	h.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		unlockErr = h.mutex.Unlock(ctx)
		closeErr := h.session.Close()
		if unlockErr == nil {
			unlockErr = closeErr
		}
	})
	return unlockErr
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

func (e *EtcdLocker) TryLock(ctx context.Context, id string) (Handle, error) {
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil, err
	}

	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	if err := mutex.TryLock(ctx); err != nil {
		_ = session.Close()
		if errors.Is(err, concurrency.ErrLocked) {
			return nil, ErrLockHeld
		}
		return nil, err
	}
	return &etcdLockHandle{mutex: mutex, session: session}, nil
}

func (e *EtcdLocker) Lock(ctx context.Context, id string) (Handle, error) {
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil, err
	}

	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	return &etcdLockHandle{mutex: mutex, session: session}, nil
}
