package lock

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lockerFactory func(t *testing.T) Locker

func TestMemoryLockerContract(t *testing.T) {
	testLockerContract(t, func(t *testing.T) Locker {
		return NewMemoryLocker()
	})
}

func TestEtcdLockerContract(t *testing.T) {
	locker, err := NewLocker("etcd://127.0.0.1:2379?prefix=/test-lock/")
	if err != nil {
		t.Skipf("etcd unavailable: %v", err)
	}
	defer locker.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	handle, err := locker.TryLock(ctx, "probe")
	if err != nil {
		t.Skipf("etcd not reachable from test sandbox: %v", err)
	}
	require.NoError(t, handle.Unlock())

	testLockerContract(t, func(t *testing.T) Locker { return locker })
}

func testLockerContract(t *testing.T, factory lockerFactory) {
	t.Run("try_lock_reports_contention", func(t *testing.T) {
		locker := factory(t)
		handle, err := locker.TryLock(context.Background(), "id")
		require.NoError(t, err)
		require.NotNil(t, handle)

		blocked, err := locker.TryLock(context.Background(), "id")
		require.ErrorIs(t, err, ErrLockHeld)
		require.Nil(t, blocked)

		require.NoError(t, handle.Unlock())
		require.NoError(t, handle.Unlock())
	})

	t.Run("lock_blocks_until_release", func(t *testing.T) {
		locker := factory(t)
		first, err := locker.Lock(context.Background(), "id")
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			second, err := locker.Lock(context.Background(), "id")
			require.NoError(t, err)
			require.NotNil(t, second)
			require.NoError(t, second.Unlock())
		}()

		select {
		case <-done:
			t.Fatal("lock acquired too early")
		case <-time.After(50 * time.Millisecond):
		}

		require.NoError(t, first.Unlock())

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for blocked lock")
		}
	})

	t.Run("context_errors_are_preserved", func(t *testing.T) {
		locker := factory(t)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		handle, err := locker.Lock(ctx, "cancelled")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, handle)

		first, err := locker.Lock(context.Background(), "busy")
		require.NoError(t, err)
		defer first.Unlock()

		waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer waitCancel()
		handle, err = locker.Lock(waitCtx, "busy")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, handle)
	})

	t.Run("concurrent_locking_is_serialized", func(t *testing.T) {
		locker := factory(t)
		defer locker.Close()

		var wg sync.WaitGroup
		var mu sync.Mutex
		seen := 0

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handle, err := locker.Lock(context.Background(), "shared")
				require.NoError(t, err)
				mu.Lock()
				seen++
				mu.Unlock()
				require.NoError(t, handle.Unlock())
			}()
		}

		wg.Wait()
		require.Equal(t, 10, seen)
	})
}

func TestNewLockerFactory(t *testing.T) {
	for _, tt := range []struct {
		url     string
		wantErr bool
	}{
		{url: "memory://"},
		{url: "local://"},
		{url: "mem://"},
		{url: "etcd://localhost:2379"},
		{url: "redis://localhost:6379", wantErr: true},
		{url: "://invalid", wantErr: true},
	} {
		t.Run(tt.url, func(t *testing.T) {
			locker, err := NewLocker(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			if strings.Contains(tt.url, "etcd") && err != nil {
				t.Skipf("etcd unavailable: %v", err)
			}
			require.NoError(t, err)
			require.NoError(t, locker.Close())
		})
	}
}

func TestEtcdLockerPathFormatting(t *testing.T) {
	locker := NewEtcdLocker(nil, "/test-lock/")
	require.Equal(t, "/test-lock/resource", locker.getLockPath("resource"))
	require.Equal(t, "/test-lock/", locker.getLockPath(""))
}

func TestUnlockDoesNotDependOnAcquireContext(t *testing.T) {
	locker := NewMemoryLocker()
	ctx, cancel := context.WithCancel(context.Background())
	handle, err := locker.Lock(ctx, "resource")
	require.NoError(t, err)
	cancel()

	require.NoError(t, handle.Unlock())

	reacquired, err := locker.TryLock(context.Background(), "resource")
	require.NoError(t, err)
	require.NoError(t, reacquired.Unlock())
}
