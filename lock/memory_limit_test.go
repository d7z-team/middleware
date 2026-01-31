package lock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemoryLocker_MaxLocks(t *testing.T) {
	// Create a locker with only 2 allowed concurrent locks
	locker := NewMemoryLockerWithConfig(16, 2)
	ctx := context.Background()

	// 1. Acquire first lock
	rel1 := locker.Lock(ctx, "id1")
	assert.NotNil(t, rel1)

	// 2. Acquire second lock
	rel2 := locker.Lock(ctx, "id2")
	assert.NotNil(t, rel2)

	// 3. Try third lock (should fail)
	rel3 := locker.TryLock(ctx, "id3")
	assert.Nil(t, rel3, "Third lock should fail due to maxLocks limit")

	// 4. Release one and try again
	rel1()
	rel4 := locker.TryLock(ctx, "id3")
	assert.NotNil(t, rel4, "Should succeed after releasing one")

	// 5. Cleanup
	rel2()
	rel4()
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
