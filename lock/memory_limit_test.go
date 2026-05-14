package lock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLocker_MaxLocks(t *testing.T) {
	// Create a locker with only 2 allowed concurrent locks
	locker := NewMemoryLockerWithConfig(16, 2)
	ctx := context.Background()

	// 1. Acquire first lock
	rel1, err := locker.Lock(ctx, "id1")
	require.NoError(t, err)
	assert.NotNil(t, rel1)

	// 2. Acquire second lock
	rel2, err := locker.Lock(ctx, "id2")
	require.NoError(t, err)
	assert.NotNil(t, rel2)

	// 3. Try third lock (should fail)
	rel3, err := locker.TryLock(ctx, "id3")
	require.ErrorIs(t, err, ErrLockHeld)
	assert.Nil(t, rel3, "Third lock should fail due to maxLocks limit")

	// 4. Release one and try again
	require.NoError(t, rel1.Unlock())
	rel4, err := locker.TryLock(ctx, "id3")
	require.NoError(t, err)
	assert.NotNil(t, rel4, "Should succeed after releasing one")

	// 5. Cleanup
	require.NoError(t, rel2.Unlock())
	require.NoError(t, rel4.Unlock())
}

func TestMemoryLocker_ConfigFromURL(t *testing.T) {
	// Test parsing shards and max from URL
	u := "memory://?shards=64&max=10"
	l, err := NewLocker(u)
	assert.NoError(t, err)

	memory, ok := l.(*MemoryLocker)
	assert.True(t, ok)
	assert.Equal(t, int32(10), memory.maxLocks)
	assert.Len(t, memory.shards, 64)
}
