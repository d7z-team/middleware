package lock

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// LockerFactory creates a locker instance for testing
type LockerFactory func(t *testing.T) Locker

// TestLocker_Common Generic test suite for Locker interface
func testLockerCommon(t *testing.T, factory LockerFactory) {
	t.Run("TryLock", func(t *testing.T) { testTryLock(t, factory) })
	t.Run("Lock", func(t *testing.T) { testLock(t, factory) })
	t.Run("ReleaseIdempotency", func(t *testing.T) { testReleaseIdempotency(t, factory) })
	t.Run("ContextCancel", func(t *testing.T) { testContextCancel(t, factory) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, factory) })
	t.Run("Boundary", func(t *testing.T) { testBoundary(t, factory) })
}

func testTryLock(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	ctx := context.Background()
	id := "try-lock-id"

	// 1. Success
	release := locker.TryLock(ctx, id)
	assert.NotNil(t, release, "First TryLock should succeed")

	// 2. Fail (already locked)
	release2 := locker.TryLock(ctx, id)
	assert.Nil(t, release2, "Second TryLock should fail")

	// 3. Success after release
	release()
	release3 := locker.TryLock(ctx, id)
	assert.NotNil(t, release3, "TryLock after release should succeed")
	release3()
}

func testLock(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	ctx := context.Background()
	id := "blocking-lock-id"

	// 1. Acquire lock
	release1 := locker.Lock(ctx, id)
	assert.NotNil(t, release1)

	// 2. Block in goroutine
	done := make(chan struct{})
	go func() {
		// Should block until release1() is called
		release2 := locker.Lock(ctx, id)
		assert.NotNil(t, release2)
		release2()
		close(done)
	}()

	// Ensure blocking (naive check)
	select {
	case <-done:
		t.Fatal("Lock acquired too early, should be blocked")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	release1()

	// 3. unblocked
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Lock timed out waiting for release")
	}
}

func testReleaseIdempotency(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	ctx := context.Background()
	id := "idempotency-id"

	release := locker.Lock(ctx, id)
	assert.NotNil(t, release)

	// Call release multiple times
	assert.NotPanics(t, func() {
		release()
		release()
		release()
	})

	// Should be able to re-acquire
	release2 := locker.TryLock(ctx, id)
	assert.NotNil(t, release2)
	release2()
}

func testContextCancel(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	id := "ctx-cancel-id"

	// 1. Cancelled before call
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Nil(t, locker.Lock(ctx, id), "Lock with canceled context should return nil")
	assert.Nil(t, locker.TryLock(ctx, id), "TryLock with canceled context should return nil")

	// 2. Cancelled while waiting
	release1 := locker.Lock(context.Background(), id)
	assert.NotNil(t, release1)
	defer release1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	start := time.Now()
	assert.Nil(t, locker.Lock(ctx2, id), "Should return nil on timeout")
	assert.WithinDuration(t, start.Add(100*time.Millisecond), time.Now(), 50*time.Millisecond)
}

func testConcurrent(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	ctx := context.Background()
	id := "concurrent-id"

	count := 0
	workers := 10
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			release := locker.Lock(ctx, id)
			if release == nil {
				return // Should not happen in blocking Lock unless error
			}
			defer release()
			count++
		}()
	}

	wg.Wait()
	assert.Equal(t, workers, count)
}

func testBoundary(t *testing.T, factory LockerFactory) {
	locker := factory(t)
	ctx := context.Background()

	// Empty ID
	release := locker.TryLock(ctx, "")
	assert.NotNil(t, release, "Empty ID should be valid")
	release()

	// Special characters
	id := "lock/with/slashes/and/spaces"
	release2 := locker.TryLock(ctx, id)
	assert.NotNil(t, release2)
	release2()
}

// --- Implementation Tests ---

func TestLocalLocker(t *testing.T) {
	testLockerCommon(t, func(t *testing.T) Locker {
		return NewLocalLocker()
	})
}

func TestEtcdLocker(t *testing.T) {
	u := "etcd://127.0.0.1:2379?prefix=/test-lock/"
	locker, err := NewLocker(u)
	if err != nil {
		t.Skipf("Skipping etcd tests: %v", err)
	}
	// Verify connection by trying a lock
	if l, ok := locker.(*EtcdLocker); ok {
		// Use a short timeout to check if etcd is reachable
		// Actually NewLocker connects, but doesn't ping.
		// Let's run the suite. If it fails due to connection, we might fail test.
		_ = l
	}

	testLockerCommon(t, func(t *testing.T) Locker {
		// Use distinct prefix or ID to avoid collision if run in parallel?
		// testLockerCommon generates IDs.
		return locker
	})
}

func TestNewLocker_Factory(t *testing.T) {
	tests := []struct {
		url     string
		wantErr bool
	}{
		{"memory://", false},
		{"local://", false},
		{"mem://", false},
		{"etcd://localhost:2379", false},
		{"redis://localhost:6379", true}, // Unsupported
		{"://invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			l, err := NewLocker(tt.url)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				// For etcd, it might succeed creation even if down
				if !strings.Contains(tt.url, "etcd") {
					assert.NoError(t, err)
					assert.NotNil(t, l)
				} else if err == nil {
					assert.NotNil(t, l)
				}
			}
		})
	}
}
