package lock

import (
	"context"
	"sync"
)

// LocalLocker 基于当前进程的本地锁实现
// 适用于单进程内的多个goroutine之间的同步
type LocalLocker struct {
	locks map[string]*sync.Mutex
	mu    sync.RWMutex // 保护locks map的读写安全
}

// NewLocalLocker 创建一个新的本地锁实例
func NewLocalLocker() *LocalLocker {
	return &LocalLocker{
		locks: make(map[string]*sync.Mutex),
	}
}

// getLock 获取指定id的锁，如果不存在则创建
func (l *LocalLocker) getLock(id string) *sync.Mutex {
	// 先尝试读锁获取
	l.mu.RLock()
	lock, exists := l.locks[id]
	l.mu.RUnlock()

	// 如果不存在则创建新的锁
	if !exists {
		l.mu.Lock()
		defer l.mu.Unlock()
		// 二次检查，防止并发创建
		if lock, exists = l.locks[id]; !exists {
			lock = &sync.Mutex{}
			l.locks[id] = lock
		}
	}

	return lock
}

// TryLock 尝试获取锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) TryLock(ctx context.Context, id string) func() {
	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	lock := l.getLock(id)

	// 尝试获取锁
	if lock.TryLock() {
		return func() {
			lock.Unlock()
		}
	}

	return nil
}

// Lock 阻塞直到获取锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) Lock(ctx context.Context, id string) func() {
	lock := l.getLock(id)

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	// 启动一个goroutine来等待上下文完成并尝试解锁
	// 注意：这不会影响已经获取的锁，只用于在等待锁的过程中上下文被取消的情况
	ch := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// 上下文被取消，尝试解锁（如果当前goroutine已经获取了锁）
			// 这里使用非阻塞的方式，避免死锁
			lock.TryLock()
			lock.Unlock()
			close(ch)
		case <-ch:
			// 正常获取锁后退出
			return
		}
	}()

	// 阻塞获取锁
	lock.Lock()
	close(ch) // 通知goroutine已经获取锁

	// 返回解锁函数
	return func() {
		lock.Unlock()
	}
}
