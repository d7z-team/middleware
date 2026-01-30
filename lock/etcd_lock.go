package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// EtcdLocker 基于etcd的分布式锁实现
// 适用于跨进程、跨机器的分布式环境下的同步
// EtcdLocker is a distributed lock implementation based on etcd.
// It is suitable for synchronization across processes and machines.
type EtcdLocker struct {
	client *clientv3.Client
	prefix string // prefix path for locks to distinguish namespaces
}

// NewEtcdLocker 创建一个新的etcd分布式锁实例
// NewEtcdLocker creates a new etcd distributed locker instance.
// client: etcd client instance
// prefix: lock prefix path, e.g., "/myapp/locks/"
func NewEtcdLocker(client *clientv3.Client, prefix string) *EtcdLocker {
	if prefix == "" {
		prefix = "/locks/"
	} else if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	return &EtcdLocker{
		client: client,
		prefix: prefix,
	}
}

// getLockPath generates the full path for the lock.
func (e *EtcdLocker) getLockPath(id string) string {
	return fmt.Sprintf("%s/%s", e.prefix, id)
}

// TryLock 尝试获取分布式锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
// TryLock tries to acquire the distributed lock without blocking.
// Returns an unlock function if successful, otherwise returns nil.
func (e *EtcdLocker) TryLock(ctx context.Context, id string) func() {
	// Create session with 60s TTL
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
			// Use a detached context for unlock to ensure it executes even if the original context is canceled
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_ = mutex.Unlock(cleanupCtx)
			_ = session.Close()
		})
	}
}

// Lock 阻塞直到获取分布式锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
// Lock blocks until the distributed lock is acquired or the context is canceled.
// Returns an unlock function if successful, otherwise returns nil.
func (e *EtcdLocker) Lock(ctx context.Context, id string) func() {
	// Create session with 60s TTL
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
			// Use a detached context for unlock to ensure it executes even if the original context is canceled
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_ = mutex.Unlock(cleanupCtx)
			_ = session.Close()
		})
	}
}
