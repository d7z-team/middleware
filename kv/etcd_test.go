package kv

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.d7z.net/middleware/connects"
)

func TestEtcdPagedKV(t *testing.T) {
	parse, _ := url.Parse("etcd://127.0.0.1:2379")

	etcd, err := connects.NewEtcd(parse)
	if err != nil {
		t.Skip("如需测试 etcd 实现，请确保本地 etcd 运行")
	}
	defer etcd.Close()

	kvClient := NewEtcd(etcd, "kv_test_list/")
	require.NoError(t, err, "创建 ETCD KV 失败")

	uniquePrefix := "kv_test_" + time.Now().Format("20060102150405.999") + "_"
	ctx := context.Background()
	cleanup := func() {
		_, _ = kvClient.client.Delete(ctx, kvClient.prefix, clientv3.WithPrefix())
	}
	defer cleanup()
	setupTestData := func(kvStore CursorPagedKV, prefix string, count int) {
		for i := 0; i < count; i++ {
			key := uniquePrefix + prefix + "_key_" + fmt.Sprintf("%03d", i)
			err := kvStore.Put(ctx, key, fmt.Sprintf("value_%d", i), TTLKeep)
			require.NoError(t, err)
		}
	}
	cleanup()
	t.Run("EmptyData", func(t *testing.T) {
		resp, err := kvClient.CursorList(ctx, &ListOptions{
			Limit: 10,
		})
		require.NoError(t, err)

		assert.Empty(t, resp.Keys)
		assert.Empty(t, resp.Cursor)
		assert.False(t, resp.HasMore)
	})
	cleanup()
	t.Run("SinglePage", func(t *testing.T) {
		setupTestData(kvClient, "test", 5)

		resp, err := kvClient.CursorList(ctx, &ListOptions{
			Limit: 10,
		})
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 5)
		assert.Empty(t, resp.Cursor)
		assert.False(t, resp.HasMore)
	})
	cleanup()
	t.Run("MultiplePages", func(t *testing.T) {
		setupTestData(kvClient, "test", 25)
		var allKeys []string
		var cursor string
		pageCount := 0

		// 第一页
		opts := &ListOptions{
			Limit:  10,
			Cursor: cursor,
		}
		resp, err := kvClient.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 10)
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)

		allKeys = append(allKeys, resp.Keys...)
		cursor = resp.Cursor
		pageCount++

		// 第二页
		opts.Cursor = cursor
		resp, err = kvClient.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 10)
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)

		allKeys = append(allKeys, resp.Keys...)
		cursor = resp.Cursor
		pageCount++

		// 第三页（最后一页）
		opts.Cursor = cursor
		resp, err = kvClient.CursorList(ctx, opts)
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
	t.Run("DefaultLimit", func(t *testing.T) {
		setupTestData(kvClient, "test", 1500) // 超过默认限制

		opts := &ListOptions{} // 不设置Limit，应该使用默认值1000

		resp, err := kvClient.CursorList(ctx, opts)
		require.NoError(t, err)

		assert.Len(t, resp.Keys, 1000)
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)
	})
	cleanup()
}
