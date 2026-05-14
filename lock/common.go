// Package lock provides local and etcd-backed distributed locking primitives.
package lock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"gopkg.d7z.net/middleware/connects"
)

// Handle releases an acquired lock.
//
// Example:
//
//	handle, _ := locker.Lock(ctx, "resource")
//	defer handle.Unlock()
type Handle interface {
	// Unlock releases the acquired lock.
	Unlock() error
}

// Locker acquires and releases named locks.
//
// Example:
//
//	locker, _ := NewLocker("memory://")
//	defer locker.Close()
//
//	handle, err := locker.TryLock(ctx, "resource")
//	if err == nil {
//		defer handle.Unlock()
//	}
//
//	blocking, err := locker.Lock(ctx, "resource-2")
//	if err == nil {
//		defer blocking.Unlock()
//	}
type Locker interface {
	io.Closer
	// TryLock acquires the named lock without waiting and returns ErrLockHeld on contention.
	TryLock(ctx context.Context, id string) (Handle, error)
	// Lock waits until the named lock is acquired or the context is canceled.
	Lock(ctx context.Context, id string) (Handle, error)
}

var ErrLockHeld = errors.New("lock already held")

// NewLocker creates a locker from a connection URL.
//
// Example:
//
//	locker, _ := NewLocker("etcd://127.0.0.1:2379?prefix=/locks/")
//	defer locker.Close()
//
//	handle, err := locker.Lock(ctx, "payments/daily-job")
//	if err == nil {
//		defer handle.Unlock()
//		// do critical work
//	}
func NewLocker(s string) (Locker, error) {
	ur, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch ur.Scheme {
	case "local", "memory", "mem":
		shards, _ := strconv.Atoi(ur.Query().Get("shards"))
		maxLocks, _ := strconv.Atoi(ur.Query().Get("max"))
		return NewMemoryLockerWithConfig(shards, maxLocks), nil
	case "etcd":
		etcd, err := connects.NewEtcd(ur)
		if err != nil {
			return nil, err
		}
		return newEtcdLocker(etcd, ur.Query().Get("prefix"), etcd.Close), nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", ur.Scheme)
	}
}
