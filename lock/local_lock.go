package lock

import (
	"context"
	"sync"
	"sync/atomic"
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
func (l *LocalLocker) getLock(id string) *atomic.Bool {
	lock, _ := l.locks.LoadOrStore(id, &atomic.Bool{})
	return lock.(*atomic.Bool)
}

// TryLock 尝试获取锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) TryLock(ctx context.Context, id string) func() {
	// 检查上下文是否已取消
	if ctx.Err() != nil {
		return nil
	}

	lock := l.getLock(id)

	// 使用 CAS (CompareAndSwap) 尝试获取锁
	if lock.CompareAndSwap(false, true) {
		var once sync.Once
		return func() {
			once.Do(func() {
				lock.Store(false)
			})
		}
	}

	return nil
}

// Lock 阻塞直到获取锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
func (l *LocalLocker) Lock(ctx context.Context, id string) func() {
	if ctx.Err() != nil {
		return nil
	}

	lock := l.getLock(id)
	// 使用 sync.Once 确保解锁函数只能被调用一次
	var once sync.Once
	unlockFunc := func() {
		once.Do(func() {
			lock.Store(false)
		})
	}

	// 快速路径：先尝试一次获取锁
	if lock.CompareAndSwap(false, true) {
		return unlockFunc
	}

	// 慢速路径：循环尝试获取锁，同时监听上下文取消
	for {
		// 在每次尝试前检查上下文是否已取消
		if ctx.Err() != nil {
			return nil
		}

		// 尝试获取锁
		if lock.CompareAndSwap(false, true) {
			return unlockFunc
		}

		// 让出CPU时间片，避免忙等待
		// 使用一个非常短暂的自旋等待，然后再次检查
		for i := 0; i < 100; i++ {
			if ctx.Err() != nil {
				return nil
			}
			// 空循环，让出时间片
		}
	}
}
