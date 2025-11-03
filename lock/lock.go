package lock

import "context"

type Locker interface {
	TryLock(ctx context.Context, id string) func()
	Lock(ctx context.Context, id string) func()
}
