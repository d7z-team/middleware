package lock

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	defaultShardCount = 4096
)

// lockEntry represents a single lock instance with reference counting.
type lockEntry struct {
	ch       chan struct{} // semaphore channel, buffer size 1
	refCount int           // reference count of active waiters/holders
}

// entryPool reuses lockEntry objects to reduce allocations.
var entryPool = sync.Pool{
	New: func() interface{} {
		return &lockEntry{
			ch: make(chan struct{}, 1),
		}
	},
}

// MemoryLocker 基于内存的分段锁实现
// 通过分段降低竞争，并使用对象池复用锁资源。
// 采用 hash/maphash 提供的安全哈希算法，有效防止针对哈希碰撞的拒绝服务攻击 (Hash DoS)。
// MemoryLocker is a sharded memory locker implementation.
// It reduces contention through sharding and reuses lock resources using a pool.
// It uses the secure hashing from hash/maphash to prevent Hash DoS attacks.
type MemoryLocker struct {
	shards     []*shard
	maxLocks   int32
	totalLocks atomic.Int32
	seed       maphash.Seed
}

type shard struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

// NewMemoryLocker 创建一个新的本地锁实例，使用默认配置。
// NewMemoryLocker creates a new memory locker instance with default configuration.
func NewMemoryLocker() *MemoryLocker {
	return NewMemoryLockerWithConfig(defaultShardCount, 0)
}

// NewMemoryLockerWithConfig 创建一个带有自定义分段数和最大锁数量限制的本地锁实例。
// shards: 分段数量，推荐为 2 的幂次。
// maxLocks: 全局最大允许的并发锁数量，0 表示不限制。
// NewMemoryLockerWithConfig creates a memory locker with custom shard count and max locks limit.
// shards: Number of shards to reduce contention.
// maxLocks: Maximum allowed concurrent locks globally, 0 for unlimited.
func NewMemoryLockerWithConfig(shards, maxLocks int) *MemoryLocker {
	if shards <= 0 {
		shards = defaultShardCount
	}
	l := &MemoryLocker{
		shards:   make([]*shard, shards),
		maxLocks: int32(maxLocks),
		seed:     maphash.MakeSeed(),
	}
	for i := range l.shards {
		l.shards[i] = &shard{
			locks: make(map[string]*lockEntry),
		}
	}
	return l
}

// hash returns a collision-resistant hash of the string
func (l *MemoryLocker) hash(s string) uint64 {
	return maphash.String(l.seed, s)
}

func (l *MemoryLocker) getShard(id string) *shard {
	return l.shards[l.hash(id)%uint64(len(l.shards))]
}

func (l *MemoryLocker) getEntry(id string) *lockEntry {
	s := l.getShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.locks[id]; ok {
		entry.refCount++
		return entry
	}

	// 检查最大锁数量限制
	// Check max locks limit
	if l.maxLocks > 0 && l.totalLocks.Load() >= l.maxLocks {
		return nil
	}

	entry := entryPool.Get().(*lockEntry)
	entry.refCount = 1
	s.locks[id] = entry
	l.totalLocks.Add(1)
	return entry
}

func (l *MemoryLocker) putEntry(id string) {
	s := l.getShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.locks[id]
	if !ok {
		return
	}

	entry.refCount--
	if entry.refCount <= 0 {
		delete(s.locks, id)
		l.totalLocks.Add(-1)
		// 确保通道状态正确（理论上释放后应该是空的）
		// Ensure channel is empty before pooling
		select {
		case <-entry.ch:
		default:
		}
		entryPool.Put(entry)
	}
}

// TryLock 尝试获取锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil（包括达到最大锁限制的情况）
// TryLock tries to acquire the lock without blocking.
// Returns an unlock function if successful, otherwise returns nil.
func (l *MemoryLocker) TryLock(ctx context.Context, id string) func() {
	if ctx.Err() != nil {
		return nil
	}

	entry := l.getEntry(id)
	if entry == nil {
		return nil
	}

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
// 如果成功获取锁，返回解锁函数；否则返回nil（包括达到最大锁限制的情况）
// Lock blocks until the lock is acquired or the context is canceled.
// Returns an unlock function if successful, otherwise returns nil.
func (l *MemoryLocker) Lock(ctx context.Context, id string) func() {
	if ctx.Err() != nil {
		return nil
	}

	entry := l.getEntry(id)
	if entry == nil {
		return nil
	}

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
