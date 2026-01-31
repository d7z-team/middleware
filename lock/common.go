// Package lock provides a unified distributed locking interface with multiple backend implementations.
// It supports both in-memory local locks for single-instance applications and etcd-based distributed locks.
//
// The package is designed to be simple to use while providing robust locking mechanisms.
package lock

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"gopkg.d7z.net/middleware/connects"
)

// Locker defines the interface for lock operations.
// It provides both non-blocking (TryLock) and blocking (Lock) lock acquisition methods.
//
// Both methods return a release function that should be called to release the lock.
// It's recommended to use defer to ensure the lock is always released:
//
//	release := locker.Lock(ctx, "my-resource")
//	defer release()
type Locker interface {
	// TryLock attempts to acquire a lock non-blockingly.
	// If the lock is successfully acquired, it returns a release function.
	// If the lock is not available, it returns nil.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - id: The unique identifier for the resource to lock
	//
	// Returns:
	//   - release function: Call this function to release the lock, or nil if lock not acquired
	//
	// Example:
	//   release := locker.TryLock(ctx, "user-123")
	//   if release != nil {
	//       defer release()
	//       // Critical section
	//   } else {
	//       // Handle lock acquisition failure
	//   }
	TryLock(ctx context.Context, id string) func()

	// Lock acquires a lock blockingly, waiting until the lock is available.
	// It returns a release function that must be called to release the lock.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeout
	//   - id: The unique identifier for the resource to lock
	//
	// Returns:
	//   - release function: Function that must be called to release the lock
	//
	// Example:
	//   release := locker.Lock(ctx, "user-123")
	//   defer release()
	//   // Critical section - guaranteed to have exclusive access
	Lock(ctx context.Context, id string) func()
}

// NewLocker creates a new Locker instance based on the provided connection string.
//
// The connection string format:
//   - Local/memory lock: "local://", "memory://", or "mem://"
//   - Etcd distributed lock: "etcd://host:port?prefix=optional_prefix"
//
// Parameters:
//   - s: Connection string specifying the lock backend and configuration
//
// Returns:
//   - Locker: Configured locker instance
//   - error: Error if the connection string is invalid or backend is unsupported
//
// Example usage:
//
//	// Local lock
//	localLocker, err := NewLocker("memory://")
//
//	// Etcd lock with default prefix
//	etcdLocker, err := NewLocker("etcd://localhost:2379")
//
//	// Etcd lock with custom prefix
//	etcdLocker, err := NewLocker("etcd://localhost:2379?prefix=myapp/")
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
		return NewEtcdLocker(etcd, ur.Query().Get("prefix")), nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", ur.Scheme)
	}
}
