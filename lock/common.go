package lock

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"gopkg.d7z.net/middleware/connects"
)

type Locker interface {
	io.Closer
	TryLock(ctx context.Context, id string) func()
	Lock(ctx context.Context, id string) func()
}

// NewLocker creates a locker from a connection URL.
//
// Example:
//
//	locker, _ := NewLocker("memory://")
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
