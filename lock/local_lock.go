package lock

import (
	"context"
	"sync"
)

// LocalLocker 基于当前进程的本地锁实现
// 适用于单进程内的多个goroutine之间的同步
// LocalLocker is a local lock implementation based on the current process.
// It is suitable for synchronization between multiple goroutines within a single process.
type LocalLocker struct {
	mu    sync.Mutex
	locks map[string]*lockEntry // active locks map
}

// lockEntry represents a single lock instance with reference counting.
type lockEntry struct {
	ch       chan struct{} // semaphore channel, buffer size 1
	refCount int           // reference count of active waiters/holders
}

// NewLocalLocker 创建一个新的本地锁实例
// NewLocalLocker creates a new local locker instance.
func NewLocalLocker() *LocalLocker {
	return &LocalLocker{
		locks: make(map[string]*lockEntry),
	}
}

// getEntry retrieves or creates a lock entry for the given ID and increments its reference count.
func (l *LocalLocker) getEntry(id string) *lockEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.locks[id]
	if !ok {
		entry = &lockEntry{
			ch:       make(chan struct{}, 1),
			refCount: 0,
		}
		l.locks[id] = entry
	}
	entry.refCount++
	return entry
}

// putEntry decrements the reference count for the lock entry and removes it if unused.
func (l *LocalLocker) putEntry(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, ok := l.locks[id]
	if !ok {
		return
	}
	entry.refCount--
	if entry.refCount <= 0 {
		delete(l.locks, id)
	}
}

// TryLock 尝试获取锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
// TryLock tries to acquire the lock without blocking.
// Returns an unlock function if successful, otherwise returns nil.
func (l *LocalLocker) TryLock(ctx context.Context, id string) func() {
	// 检查上下文是否已取消
	if ctx.Err() != nil {
		return nil
	}

	entry := l.getEntry(id)

	select {
	case entry.ch <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-entry.ch
				l.putEntry(id)
			})
		}
	default:
		l.putEntry(id)
		return nil
	}
}

// Lock 阻塞直到获取锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
// Lock blocks until the lock is acquired or the context is canceled.
// Returns an unlock function if successful, otherwise returns nil.
func (l *LocalLocker) Lock(ctx context.Context, id string) func() {
	if ctx.Err() != nil {
		return nil
	}

	entry := l.getEntry(id)

	select {
	case entry.ch <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() {
				<-entry.ch
				l.putEntry(id)
			})
		}
	case <-ctx.Done():
		l.putEntry(id)
		return nil
	}
}
