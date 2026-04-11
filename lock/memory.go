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

type lockEntry struct {
	ch       chan struct{}
	refCount int
}

var entryPool = sync.Pool{
	New: func() interface{} {
		return &lockEntry{
			ch: make(chan struct{}, 1),
		}
	},
}

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

// NewMemoryLocker creates a memory locker with default settings.
//
// Example:
//
//	locker := NewMemoryLocker()
func NewMemoryLocker() *MemoryLocker {
	return NewMemoryLockerWithConfig(defaultShardCount, 0)
}

// NewMemoryLockerWithConfig creates a memory locker with custom shard and limit settings.
//
// Example:
//
//	locker := NewMemoryLockerWithConfig(1024, 10000)
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

func (l *MemoryLocker) Close() error {
	return nil
}

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
		select {
		case <-entry.ch:
		default:
		}
		entryPool.Put(entry)
	}
}

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
