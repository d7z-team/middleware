package lock

import (
	"context"
	"fmt"
	"net/url"

	"gopkg.d7z.net/middleware/connects"
)

type Locker interface {
	TryLock(ctx context.Context, id string) func()
	Lock(ctx context.Context, id string) func()
}

func NewLocker(s string) (Locker, error) {
	ur, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch ur.Scheme {
	case "local", "memory", "mem":
		return NewLocalLocker(), nil
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
