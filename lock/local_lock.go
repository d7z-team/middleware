package lock

import (
	"context"
	"sync"
)

// LocalLocker 基于当前进程的本地锁实现
// 适用于单进程内的多个goroutine之间的同步
type LocalLocker struct {
	locks sync.Map // 使用 sync.Map 存储锁
}

// NewLocalLocker 创建一个新的本地锁实例
func NewLocalLocker() *LocalLocker {
	return &LocalLocker{}
}

// getLock 获取指定id的锁，如果不存在则创建
func (l *LocalLocker) getLock(id string) *sync.Mutex {
	lock, _ := l.locks.LoadOrStore(id, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// TryLock 尝试获取锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) TryLock(ctx context.Context, id string) func() {
	// 检查上下文是否已取消
	if ctx.Err() != nil {
		return nil
	}

	lock := l.getLock(id)

	// 尝试获取锁
	if lock.TryLock() {
		var once sync.Once
		return func() {
			once.Do(lock.Unlock)
		}
	}

	return nil
}

// Lock 阻塞直到获取锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) Lock(ctx context.Context, id string) func() {
	lock := l.getLock(id)
	// 使用通道来协调锁获取和上下文取消
	done := make(chan struct{})
	defer close(done)

	// 使用 sync.Once 确保解锁函数只能被调用一次
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			// 上下文取消，尝试解锁（如果当前goroutine已经获取了锁）
			lock.TryLock()       // 确保我们有锁的所有权
			once.Do(lock.Unlock) // 然后立即释放
		case <-done:
			// 正常退出
		}
	}()

	// 阻塞获取锁
	lock.Lock()

	// 检查上下文是否在获取锁的过程中被取消
	if ctx.Err() != nil {
		once.Do(lock.Unlock)
		return nil
	}

	return func() {
		once.Do(lock.Unlock)
	}
}
