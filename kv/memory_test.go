package kv

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryPagedKV(t *testing.T) {
	memory, err := NewMemory("")
	if err != nil {
		t.Skip("如需测试 etcd 实现，请确保本地 etcd 运行")
	}

	require.NoError(t, err, "创建 ETCD KV 失败")

	uniquePrefix := "kv_test_" + time.Now().Format("20060102150405.999") + "_"
	ctx := context.Background()
	cleanup := func() {
		memory.data.Clear()
	}
	defer cleanup()
	setupTestData := func(kvStore CursorPagedKV, prefix string, count int) {
		for i := 0; i < count; i++ {
			key := uniquePrefix + prefix + "_key_" + fmt.Sprintf("%03d", i)
			err := kvStore.Put(ctx, key, "value_"+strconv.Itoa(i), TTLKeep)
			require.NoError(t, err)
		}
	}
	cleanup()
	t.Run("EmptyData", func(t *testing.T) {
		resp, err := memory.CursorList(ctx, &ListOptions{
			Limit: 10,
		})
		require.NoError(t, err)

		assert.Empty(t, resp.Keys)
		assert.Empty(t, resp.Cursor)
		assert.False(t, resp.HasMore)
	})
	cleanup()
	t.Run("SinglePage", func(t *testing.T) {
		setupTestData(memory, "test", 5)

		resp, err := memory.CursorList(ctx, &ListOptions{
			Limit: 10,
		})
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 5)
		assert.Empty(t, resp.Cursor)
		assert.False(t, resp.HasMore)
	})
	cleanup()
	t.Run("MultiplePages", func(t *testing.T) {
		setupTestData(memory, "test", 25)
		var allKeys []string
		var cursor string
		pageCount := 0

		// 第一页
		opts := &ListOptions{
			Limit:  10,
			Cursor: cursor,
		}
		resp, err := memory.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 10)
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)

		allKeys = append(allKeys, resp.Keys...)
		cursor = resp.Cursor
		pageCount++

		// 第二页
		opts.Cursor = cursor
		resp, err = memory.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 10)
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)

		allKeys = append(allKeys, resp.Keys...)
		cursor = resp.Cursor
		pageCount++

		// 第三页（最后一页）
		opts.Cursor = cursor
		resp, err = memory.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 5)
		assert.False(t, resp.HasMore)
		assert.Empty(t, resp.Cursor)

		allKeys = append(allKeys, resp.Keys...)
		pageCount++

		assert.Equal(t, 3, pageCount)
		assert.Len(t, allKeys, 25)
		assertNoDuplicates(t, allKeys)
	})
	cleanup()
}
