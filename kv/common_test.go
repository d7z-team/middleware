package kv

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/connects"
)

// testKVConsistency is the abstract test suite that all KV implementations must pass.
func testKVConsistency(t *testing.T, kvClient KV) {
	t.Helper()
	ctx := t.Context()
	// Unique prefix to avoid collisions between test runs
	uniquePrefix := "kv_test_" + time.Now().Format("20060102150405.000") + "_"

	// Basic Operations
	t.Run("Basic_Operations", func(t *testing.T) {
		key := uniquePrefix + "basic"
		val := "value_basic"

		// Put
		err := kvClient.Put(ctx, key, val, TTLKeep)
		assert.NoError(t, err)

		// Get
		got, err := kvClient.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, val, got)

		// Delete
		deleted, err := kvClient.Delete(ctx, key)
		assert.NoError(t, err)
		assert.True(t, deleted)

		// Get after Delete
		_, err = kvClient.Get(ctx, key)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		// Delete non-existent
		deleted, err = kvClient.Delete(ctx, key)
		assert.NoError(t, err)
		assert.False(t, deleted)
	})

	// Boundary Conditions
	t.Run("Boundary_Conditions", func(t *testing.T) {
		// Empty Value
		keyEmptyVal := uniquePrefix + "empty_val"
		err := kvClient.Put(ctx, keyEmptyVal, "", TTLKeep)
		assert.NoError(t, err)
		got, err := kvClient.Get(ctx, keyEmptyVal)
		assert.NoError(t, err)
		assert.Equal(t, "", got)

		// Keys with special characters
		// Note: We expect all implementations to handle these (including slashes)
		specialKeys := map[string]string{
			uniquePrefix + "space key":   "space",
			uniquePrefix + "dash-key":    "dash",
			uniquePrefix + "under_score": "underscore",
			uniquePrefix + "slash/key":   "slash", // Hierarchy implied
			uniquePrefix + "中文key":       "unicode",
			uniquePrefix + "dot.key":     "dot",
			uniquePrefix + "eq=key":      "equal",
		}

		for k, v := range specialKeys {
			err := kvClient.Put(ctx, k, v, TTLKeep)
			assert.NoError(t, err, "Put failed for key: %s", k)
			got, err := kvClient.Get(ctx, k)
			assert.NoError(t, err, "Get failed for key: %s", k)
			assert.Equal(t, v, got)
		}
	})

	// TTL Handling
	t.Run("TTL_Handling", func(t *testing.T) {
		key := uniquePrefix + "ttl_short"
		// 1 second TTL
		err := kvClient.Put(ctx, key, "ttl_val", 1*time.Second)
		assert.NoError(t, err)

		// Should exist immediately
		_, err = kvClient.Get(ctx, key)
		assert.NoError(t, err)

		// Wait for expiration (allow some buffer)
		assert.Eventually(t, func() bool {
			_, err := kvClient.Get(ctx, key)
			return errors.Is(err, ErrKeyNotFound)
		}, 3*time.Second, 200*time.Millisecond, "Key should expire")

		// Test TTLKeep
		keyKeep := uniquePrefix + "ttl_keep"
		// Set with 2s TTL
		err = kvClient.Put(ctx, keyKeep, "val1", 2*time.Second)
		assert.NoError(t, err)

		// Update value with TTLKeep (should preserve ~2s TTL)
		err = kvClient.Put(ctx, keyKeep, "val2", TTLKeep)
		assert.NoError(t, err)

		got, err := kvClient.Get(ctx, keyKeep)
		assert.NoError(t, err)
		assert.Equal(t, "val2", got)

		// Wait for it to expire
		assert.Eventually(t, func() bool {
			_, err := kvClient.Get(ctx, keyKeep)
			return errors.Is(err, ErrKeyNotFound)
		}, 3*time.Second, 200*time.Millisecond, "Key should expire after original TTL")

		// Test TTLKeep on New Key (should be permanent or default)
		keyNewKeep := uniquePrefix + "ttl_new_keep"
		err = kvClient.Put(ctx, keyNewKeep, "val_new_keep", TTLKeep)
		assert.NoError(t, err)
		got, err = kvClient.Get(ctx, keyNewKeep)
		assert.NoError(t, err)
		assert.Equal(t, "val_new_keep", got)
	})

	// Put If Not Exists
	t.Run("Put_If_Not_Exists", func(t *testing.T) {
		key := uniquePrefix + "nx_key"
		val1 := "v1"
		val2 := "v2"

		// Success
		ok, err := kvClient.PutIfNotExists(ctx, key, val1, TTLKeep)
		assert.NoError(t, err)
		assert.True(t, ok)

		got, err := kvClient.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, val1, got)

		// Failure (already exists)
		ok, err = kvClient.PutIfNotExists(ctx, key, val2, TTLKeep)
		assert.NoError(t, err)
		assert.False(t, ok)

		got, err = kvClient.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, val1, got) // Should not change
	})

	// Compare And Swap
	t.Run("Compare_And_Swap", func(t *testing.T) {
		key := uniquePrefix + "cas_key"
		_ = kvClient.Put(ctx, key, "old", TTLKeep)

		// Success
		ok, err := kvClient.CompareAndSwap(ctx, key, "old", "new")
		assert.NoError(t, err)
		assert.True(t, ok)
		got, _ := kvClient.Get(ctx, key)
		assert.Equal(t, "new", got)

		// Failure (value mismatch)
		ok, err = kvClient.CompareAndSwap(ctx, key, "wrong", "fail")
		assert.NoError(t, err)
		assert.False(t, ok)
		got, _ = kvClient.Get(ctx, key)
		assert.Equal(t, "new", got)

		// Failure (key not found)
		ok, err = kvClient.CompareAndSwap(ctx, uniquePrefix+"missing", "any", "val")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrKeyNotFound) || errors.Is(err, os.ErrNotExist), "Should return KeyNotFound error")
	})

	// Child & Hierarchy Isolation
	t.Run("Child_And_Hierarchy_Isolation", func(t *testing.T) {
		// Parent: uniquePrefix
		// Child: uniquePrefix + "sub/"
		child := kvClient.Child(uniquePrefix + "sub")

		// Put in child
		err := child.Put(ctx, "key", "child_val", TTLKeep)
		assert.NoError(t, err)

		// Get from child
		got, err := child.Get(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, "child_val", got)

		// Let's test relative usage which is cleaner.
		subKV := kvClient.Child(uniquePrefix + "child")
		err = subKV.Put(ctx, "a", "val_a", TTLKeep)
		assert.NoError(t, err)

		// We Put `uniquePrefix + "child/direct_put"` into kvClient.
		directKey := uniquePrefix + "child/direct_put"
		err = kvClient.Put(ctx, directKey, "direct_val", TTLKeep)
		assert.NoError(t, err)

		// Create child for that path
		cKV := kvClient.Child(uniquePrefix + "child")
		// Should be able to get "direct_put"
		got, err = cKV.Get(ctx, "direct_put")
		assert.NoError(t, err)
		assert.Equal(t, "direct_val", got)
	})

	// Offset-based Pagination
	t.Run("Offset_Pagination", func(t *testing.T) {
		prefix := uniquePrefix + "paged_"
		// Insert 15 items: 01, 02, ..., 15
		for i := 1; i <= 15; i++ {
			k := fmt.Sprintf("%s%02d", prefix, i)
			_ = kvClient.Put(ctx, k, fmt.Sprintf("val_%02d", i), TTLKeep)
		}

		// Test List (All)
		all, err := kvClient.List(ctx, prefix)
		assert.NoError(t, err)
		assert.Len(t, all, 15)
		assert.Equal(t, "val_01", all[prefix+"01"])

		// Test ListPage
		// Page 1 (size 10) -> 1-10
		page1, err := kvClient.ListPage(ctx, prefix, 0, 10)
		assert.NoError(t, err)
		assert.Len(t, page1, 10)
		// Verify order (assuming lexicographical)
		// keys: 01, 02 ... 10
		_, ok01 := page1[prefix+"01"]
		assert.True(t, ok01)
		_, ok10 := page1[prefix+"10"]
		assert.True(t, ok10)

		// Page 2 (size 10) -> 11-15
		page2, err := kvClient.ListPage(ctx, prefix, 1, 10)
		assert.NoError(t, err)
		assert.Len(t, page2, 5)
		_, ok11 := page2[prefix+"11"]
		assert.True(t, ok11)

		// Page 3 (size 10) -> Empty
		page3, err := kvClient.ListPage(ctx, prefix, 2, 10)
		assert.NoError(t, err)
		assert.Empty(t, page3)
	})

	// Cursor-based Pagination
	t.Run("Cursor_Pagination", func(t *testing.T) {
		// Insert 5 items: a, b, c, d, e
		keys := []string{"a", "b", "c", "d", "e"}

		// Clean up and re-insert using Child
		child := kvClient.Child(uniquePrefix + "cursor") // .../cursor/

		for _, k := range keys {
			_ = child.Put(ctx, k, "val_"+k, TTLKeep)
		}

		// 1. Get first 2
		opts := &ListOptions{
			Limit: 2,
		}

		// First page
		resp, err := child.CursorList(ctx, opts)
		assert.NoError(t, err)
		assert.Len(t, resp.Keys, 2)
		assert.Equal(t, "a", resp.Keys[0])
		assert.Equal(t, "b", resp.Keys[1])
		assert.True(t, resp.HasMore)
		assert.NotEmpty(t, resp.Cursor)
		// Cursor should be "b" (last key)
		assert.Equal(t, "b", resp.Cursor)

		// Second page
		opts.Cursor = resp.Cursor
		resp2, err := child.CursorList(ctx, opts)
		assert.NoError(t, err)
		assert.Len(t, resp2.Keys, 2)
		assert.Equal(t, "c", resp2.Keys[0])
		assert.Equal(t, "d", resp2.Keys[1])
		assert.True(t, resp2.HasMore)

		// Third page (last item)
		opts.Cursor = resp2.Cursor
		resp3, err := child.CursorList(ctx, opts)
		assert.NoError(t, err)
		assert.Len(t, resp3.Keys, 1)
		assert.Equal(t, "e", resp3.Keys[0])

		// Fourth page (empty)
		if resp3.HasMore {
			opts.Cursor = resp3.Cursor
			resp4, err := child.CursorList(ctx, opts)
			assert.NoError(t, err)
			assert.Empty(t, resp4.Keys)
		}
	})

	// Key Count Verification
	t.Run("Key_Count_Verification", func(t *testing.T) {
		prefix := uniquePrefix + "count_root"
		child := kvClient.Child(prefix)

		// 0 count
		c, err := child.Count(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), c)

		// Add items
		_ = child.Put(ctx, "k1", "v", TTLKeep)
		_ = child.Put(ctx, "k2", "v", TTLKeep)
		_ = child.Put(ctx, "sub/k3", "v", TTLKeep) // Recursive count check

		c, err = child.Count(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), c)
	})

	// Delete All
	t.Run("Delete_All", func(t *testing.T) {
		daPrefix := uniquePrefix + "delete_all"
		daKV := kvClient.Child(daPrefix)

		// Populate
		keys := []string{"k1", "k2", "sub/k3"}
		for _, k := range keys {
			err := daKV.Put(ctx, k, "val", TTLKeep)
			require.NoError(t, err)
		}

		// Verify count before
		c, err := daKV.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(3), c)

		// DeleteAll
		err = daKV.DeleteAll(ctx)
		require.NoError(t, err)

		// Verify count after
		c, err = daKV.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(0), c)

		// Verify individual keys are gone
		for _, k := range keys {
			_, err := daKV.Get(ctx, k)
			require.ErrorIs(t, err, ErrKeyNotFound)
		}
	})

	// Concurrency Safety
	t.Run("Concurrency_Safe", func(t *testing.T) {
		var wg sync.WaitGroup
		concurrency := 10
		ops := 50
		prefix := uniquePrefix + "conc/"

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < ops; j++ {
					key := fmt.Sprintf("%s%d-%d", prefix, id, j)
					err := kvClient.Put(ctx, key, "val", TTLKeep)
					assert.NoError(t, err)

					_, err = kvClient.Get(ctx, key)
					assert.NoError(t, err)
				}
			}(i)
		}
		wg.Wait()

		// Verify count
		count, err := kvClient.Child(uniquePrefix + "conc").Count(ctx)
		assert.NoError(t, err)
		assert.Equal(t, int64(concurrency*ops), count)
	})

	// Context Cancellation
	t.Run("Context_Cancellation", func(t *testing.T) {
		cCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		key := uniquePrefix + "ctx_cancel"
		err := kvClient.Put(cCtx, key, "val", TTLKeep)
		// Operations should fail with context canceled or similar error
		assert.Error(t, err)
	})

	// Deep Nested Child
	t.Run("Deep_Nested_Child", func(t *testing.T) {
		// root -> l1 -> l2 -> l3
		l1 := kvClient.Child(uniquePrefix + "level1")
		l2 := l1.Child("level2")
		l3 := l2.Child("level3")

		key := "deep_key"
		err := l3.Put(ctx, key, "deep_val", TTLKeep)
		assert.NoError(t, err)

		// Verify visibility from top
		fullKey := uniquePrefix + "level1/level2/level3/" + key
		val, err := kvClient.Get(ctx, fullKey)
		assert.NoError(t, err)
		assert.Equal(t, "deep_val", val)
	})

	// DeleteAll Isolation
	t.Run("DeleteAll_Isolation", func(t *testing.T) {
		// structure:
		// prefix/keep/k1
		// prefix/del/k1

		keepKV := kvClient.Child(uniquePrefix + "keep")
		delKV := kvClient.Child(uniquePrefix + "del")

		_ = keepKV.Put(ctx, "k1", "v", TTLKeep)
		_ = delKV.Put(ctx, "k1", "v", TTLKeep)

		err := delKV.DeleteAll(ctx)
		assert.NoError(t, err)

		// Verify del is gone
		c, _ := delKV.Count(ctx)
		assert.Equal(t, int64(0), c)

		// Verify keep is there
		val, err := keepKV.Get(ctx, "k1")
		assert.NoError(t, err)
		assert.Equal(t, "v", val)
	})

	// Complex Hierarchy and Pagination
	t.Run("Hierarchy_And_Pagination_Scenarios", func(t *testing.T) {
		// Use a sub-namespace for this test
		// prefix/hierarchy/
		hPrefix := uniquePrefix + "hierarchy"
		hKV := kvClient.Child(hPrefix)

		// Data:
		// a, a/c1, a/c2, b
		keys := []string{"a", "a/c1", "a/c2", "b"}
		for _, k := range keys {
			err := hKV.Put(ctx, k, "val_"+k, TTLKeep)
			require.NoError(t, err)
		}

		// 2. 验证父级分页 (CursorList)
		t.Run("Parent_Pagination", func(t *testing.T) {
			opts := &ListOptions{Limit: 1}

			// Page 1 -> "a"
			resp1, err := hKV.CursorList(ctx, opts)
			require.NoError(t, err)
			require.Len(t, resp1.Keys, 1)
			assert.Equal(t, "a", resp1.Keys[0])
			assert.True(t, resp1.HasMore)
			assert.Equal(t, "a", resp1.Cursor)

			// Page 2 -> "a/c1"
			opts.Cursor = resp1.Cursor
			resp2, err := hKV.CursorList(ctx, opts)
			require.NoError(t, err)
			require.Len(t, resp2.Keys, 1)
			assert.Equal(t, "a/c1", resp2.Keys[0])
			assert.True(t, resp2.HasMore)

			// Page 3 -> "a/c2"
			opts.Cursor = resp2.Cursor
			resp3, err := hKV.CursorList(ctx, opts)
			require.NoError(t, err)
			require.Len(t, resp3.Keys, 1)
			assert.Equal(t, "a/c2", resp3.Keys[0])

			// Page 4 -> "b"
			opts.Cursor = resp3.Cursor
			resp4, err := hKV.CursorList(ctx, opts)
			require.NoError(t, err)
			require.Len(t, resp4.Keys, 1)
			assert.Equal(t, "b", resp4.Keys[0])
			assert.False(t, resp4.HasMore)
			assert.Empty(t, resp4.Cursor)
		})

		// 3. 验证 Child 视图
		t.Run("Child_View", func(t *testing.T) {
			childKV := hKV.Child("a")

			list, err := childKV.List(ctx, "")
			require.NoError(t, err)
			assert.Len(t, list, 2)
			assert.Contains(t, list, "c1")
			assert.Contains(t, list, "c2")

			_, exists := list[""]
			assert.False(t, exists, "Should not see parent key 'a' in child view 'a/'")

			val, err := childKV.Get(ctx, "c1")
			require.NoError(t, err)
			assert.Equal(t, "val_a/c1", val)
		})

		// 4. 验证 Child 写入的互通性
		t.Run("Child_Interoperability", func(t *testing.T) {
			childKV := hKV.Child("a")
			err := childKV.Put(ctx, "c3", "val_child_put", TTLKeep)
			require.NoError(t, err)

			val, err := hKV.Get(ctx, "a/c3")
			require.NoError(t, err)
			assert.Equal(t, "val_child_put", val)
		})

		// 5. 验证多级 Child ("a/b")
		t.Run("Multi_Level_Child", func(t *testing.T) {
			childMulti := hKV.Child("x/y")
			err := childMulti.Put(ctx, "key", "val_multi", TTLKeep)
			require.NoError(t, err)

			childStep := hKV.Child("x", "y")
			val, err := childStep.Get(ctx, "key")
			require.NoError(t, err)
			assert.Equal(t, "val_multi", val)

			valParent, err := hKV.Get(ctx, "x/y/key")
			require.NoError(t, err)
			assert.Equal(t, "val_multi", valParent)

			childChained := hKV.Child("x").Child("y")
			valChain, err := childChained.Get(ctx, "key")
			require.NoError(t, err)
			assert.Equal(t, "val_multi", valChain)
		})
	})
}

func TestNewKVFromURL(t *testing.T) {
	// 1. Memory
	t.Run("Memory", func(t *testing.T) {
		kv, err := NewKVFromURL("memory://")
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.NoError(t, kv.Close())

		kv2, err := NewKVFromURL("mem://")
		require.NoError(t, err)
		require.NotNil(t, kv2)
		require.NoError(t, kv2.Close())
	})

	// 2. Storage/Local
	t.Run("Storage", func(t *testing.T) {
		tmp := t.TempDir()
		path := filepath.Join(tmp, "kv.json")

		// storage://path
		// URL parsing might be tricky with absolute paths.
		// Usually: storage:///tmp/kv.json (3 slashes for local file on unix)
		// Or relative: storage://./kv.json
		// Implementation uses parse.Path.

		kv, err := NewKVFromURL("storage://" + path)
		require.NoError(t, err)
		require.NotNil(t, kv)
		require.NoError(t, kv.Close()) // Syncs

		kv2, err := NewKVFromURL("local://" + path)
		require.NoError(t, err)
		require.NotNil(t, kv2)
		require.NoError(t, kv2.Close())
	})

	// 3. Etcd (Mock/Check)
	t.Run("Etcd", func(t *testing.T) {
		// Just check if it attempts connection or validates URL
		// We expect error if etcd is not running on random port, or success if it connects.
		// "etcd://127.0.0.1:2379" might work if local etcd is up.
		kv, err := NewKVFromURL("etcd://127.0.0.1:2379?prefix=test")
		// If local etcd is up, err is nil. If not, err might be nil but ops fail, OR NewEtcd fails on Dial?
		// NewEtcd uses Dial which is non-blocking usually unless configured otherwise.
		// connects.NewEtcd might wait.
		if err == nil {
			assert.NotNil(t, kv)
			_ = kv.Close()
		}
	})

	// 4. Redis (Mock/Check)
	t.Run("Redis", func(t *testing.T) {
		kv, err := NewKVFromURL("redis://localhost:6379/0?prefix=test")
		if err == nil {
			assert.NotNil(t, kv)
			_ = kv.Close()
		}
	})

	// 5. Invalid
	t.Run("Invalid", func(t *testing.T) {
		_, err := NewKVFromURL("unknown://")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported scheme")

		_, err = NewKVFromURL(":%") // Invalid URL
		require.Error(t, err)
	})
}

// ########################### Implementation Tests ###########################

func TestMemoryKV(t *testing.T) {
	// 1. Pure Memory
	t.Run("Memory", func(t *testing.T) {
		kv, err := NewMemory("")
		require.NoError(t, err)
		testKVConsistency(t, kv)
	})

	// 2. File Persistence
	t.Run("FileStorage", func(t *testing.T) {
		tmpDir := t.TempDir()
		storePath := filepath.Join(tmpDir, "kv.json")
		kv, err := NewMemory(storePath)
		require.NoError(t, err)

		testKVConsistency(t, kv)

		// Verify persistence
		key := "persist_key"
		val := "persist_val"
		_ = kv.Put(context.Background(), key, val, TTLKeep)
		_ = kv.Sync()

		// Re-open
		kv2, err := NewMemory(storePath)
		require.NoError(t, err)
		got, err := kv2.Get(context.Background(), key)
		assert.NoError(t, err)
		assert.Equal(t, val, got)
	})
}

func TestRedisKV(t *testing.T) {
	// Ensure local Redis is available or skip
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer client.Close()

	kv := NewRedis(client, "test_kv_redis/")
	testKVConsistency(t, kv)
}

func TestEtcdKV(t *testing.T) {
	// Ensure local Etcd is available or skip
	u, _ := url.Parse("etcd://127.0.0.1:2379")
	etcdClient, err := connects.NewEtcd(u)
	if err != nil {
		t.Skipf("Etcd not available: %v", err)
	}
	defer etcdClient.Close()

	kv := NewEtcd(etcdClient, "test_kv_etcd/")
	testKVConsistency(t, kv)
}
