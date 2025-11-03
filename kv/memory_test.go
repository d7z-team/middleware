package kv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 辅助函数：创建临时文件路径
func tempFilePath(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	return filepath.Join(tempDir, "kv_test.json")
}

// TestNewKVMemory 测试创建KVMemory实例
func TestNewKVMemory(t *testing.T) {
	t.Run("无持久化存储", func(t *testing.T) {
		kv, err := NewMemory("")
		if err != nil {
			t.Fatalf("创建无存储实例失败: %v", err)
		}
		if kv == nil {
			t.Fatal("返回的KV实例为nil")
		}
	})

	t.Run("有持久化存储且文件不存在", func(t *testing.T) {
		path := tempFilePath(t)
		kv, err := NewMemory(path)
		if err != nil {
			t.Fatalf("创建带存储实例失败: %v", err)
		}
		if kv == nil {
			t.Fatal("返回的KV实例为nil")
		}
	})

	t.Run("有持久化存储且文件存在", func(t *testing.T) {
		path := tempFilePath(t)
		// 先创建并写入数据
		initialKV, err := NewMemory(path)
		if err != nil {
			t.Fatalf("初始化存储失败: %v", err)
		}
		if err := initialKV.Put(context.Background(), "testkey", "testval", -1); err != nil {
			t.Fatalf("写入初始数据失败: %v", err)
		}
		if err := initialKV.Close(); err != nil {
			t.Fatalf("关闭初始存储失败: %v", err)
		}

		// 重新加载
		kv, err := NewMemory(path)
		if err != nil {
			t.Fatalf("重新加载存储失败: %v", err)
		}
		val, err := kv.Get(context.Background(), "testkey")
		if err != nil {
			t.Fatalf("获取重新加载的数据失败: %v", err)
		}
		if val != "testval" {
			t.Errorf("期望值为'testval'，实际为'%s'", val)
		}
	})

	t.Run("持久化存储文件格式错误", func(t *testing.T) {
		path := tempFilePath(t)
		// 写入错误格式数据
		if err := os.WriteFile(path, []byte("invalid json"), 0o644); err != nil {
			t.Fatalf("写入错误格式文件失败: %v", err)
		}

		_, err := NewMemory(path)
		if err == nil {
			t.Error("预期加载错误格式文件会失败，但未返回错误")
		}
	})
}

// TestPutAndGet 测试Put和Get方法
func TestPutAndGet(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()

	t.Run("存储并获取未过期键", func(t *testing.T) {
		key, val := "key1", "val1"
		if err := kv.Put(ctx, key, val, -1); err != nil {
			t.Fatalf("Put失败: %v", err)
		}

		result, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get失败: %v", err)
		}
		if result != val {
			t.Errorf("期望值为'%s'，实际为'%s'", val, result)
		}
	})

	t.Run("获取不存在的键", func(t *testing.T) {
		_, err := kv.Get(ctx, "nonexistent")
		if err != os.ErrNotExist {
			t.Errorf("期望错误为os.ErrNotExist，实际为%v", err)
		}
	})

	t.Run("存储带过期时间的键", func(t *testing.T) {
		key := "expiring_key"
		if err := kv.Put(ctx, key, "expire_soon", time.Millisecond*100); err != nil {
			t.Fatalf("Put失败: %v", err)
		}

		// 立即获取
		if _, err := kv.Get(ctx, key); err != nil {
			t.Error("过期前获取失败:", err)
		}

		// 等待过期
		time.Sleep(time.Millisecond * 150)

		// 过期后获取
		_, err := kv.Get(ctx, key)
		if err != os.ErrNotExist {
			t.Errorf("期望过期后返回os.ErrNotExist，实际为%v", err)
		}
	})
}

// TestDelete 测试Delete方法
func TestDelete(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()
	key := "delete_key"

	t.Run("删除存在的键", func(t *testing.T) {
		if err := kv.Put(ctx, key, "to_delete", -1); err != nil {
			t.Fatalf("Put失败: %v", err)
		}

		deleted, err := kv.Delete(ctx, key)
		if err != nil {
			t.Fatalf("Delete失败: %v", err)
		}
		if !deleted {
			t.Error("Delete返回false，但键应该存在")
		}

		_, err = kv.Get(ctx, key)
		if err != os.ErrNotExist {
			t.Errorf("删除后获取键，期望os.ErrNotExist，实际为%v", err)
		}
	})

	t.Run("删除不存在的键", func(t *testing.T) {
		deleted, err := kv.Delete(ctx, "nonexistent")
		if err != nil {
			t.Fatalf("Delete失败: %v", err)
		}
		if deleted {
			t.Error("Delete返回true，但键不存在")
		}
	})
}

// TestPutIfNotExists 测试PutIfNotExists方法
func TestPutIfNotExists(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()
	key := "put_if_not_exists_key"

	t.Run("键不存在时插入成功", func(t *testing.T) {
		ok, err := kv.PutIfNotExists(ctx, key, "value", -1)
		if err != nil {
			t.Fatalf("PutIfNotExists失败: %v", err)
		}
		if !ok {
			t.Error("PutIfNotExists返回false，但键应该不存在")
		}

		val, _ := kv.Get(ctx, key)
		if val != "value" {
			t.Errorf("期望值为'value'，实际为'%s'", val)
		}
	})

	t.Run("键存在时插入失败", func(t *testing.T) {
		ok, err := kv.PutIfNotExists(ctx, key, "new_value", -1)
		if err != nil {
			t.Fatalf("PutIfNotExists失败: %v", err)
		}
		if ok {
			t.Error("PutIfNotExists返回true，但键应该已存在")
		}

		val, _ := kv.Get(ctx, key)
		if val != "value" {
			t.Errorf("期望值保持'value'，实际为'%s'", val)
		}
	})

	t.Run("键已过期时插入成功", func(t *testing.T) {
		expireKey := "expired_key"
		// 插入一个马上过期的键
		if err := kv.Put(ctx, expireKey, "old", time.Millisecond*10); err != nil {
			t.Fatalf("Put失败: %v", err)
		}
		time.Sleep(time.Millisecond * 20)

		ok, err := kv.PutIfNotExists(ctx, expireKey, "new", -1)
		if err != nil {
			t.Fatalf("PutIfNotExists失败: %v", err)
		}
		if !ok {
			t.Error("PutIfNotExists返回false，但过期键应该被替换")
		}

		val, _ := kv.Get(ctx, expireKey)
		if val != "new" {
			t.Errorf("期望值为'new'，实际为'%s'", val)
		}
	})
}

// TestCompareAndSwap 测试CompareAndSwap方法
func TestCompareAndSwap(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()
	key := "cas_key"

	t.Run("键不存在时失败", func(t *testing.T) {
		ok, err := kv.CompareAndSwap(ctx, key, "old", "new")
		if err != os.ErrNotExist {
			t.Errorf("期望错误为os.ErrNotExist，实际为%v", err)
		}
		if ok {
			t.Error("CompareAndSwap返回true，但键不存在")
		}
	})

	t.Run("值不匹配时失败", func(t *testing.T) {
		if err := kv.Put(ctx, key, "original", -1); err != nil {
			t.Fatalf("Put失败: %v", err)
		}

		ok, err := kv.CompareAndSwap(ctx, key, "wrong_old", "new")
		if err != nil {
			t.Fatalf("CompareAndSwap失败: %v", err)
		}
		if ok {
			t.Error("CompareAndSwap返回true，但值不匹配")
		}

		val, _ := kv.Get(ctx, key)
		if val != "original" {
			t.Errorf("期望值保持'original'，实际为'%s'", val)
		}
	})

	t.Run("值匹配时成功替换", func(t *testing.T) {
		ok, err := kv.CompareAndSwap(ctx, key, "original", "updated")
		if err != nil {
			t.Fatalf("CompareAndSwap失败: %v", err)
		}
		if !ok {
			t.Error("CompareAndSwap返回false，但值应该匹配")
		}

		val, _ := kv.Get(ctx, key)
		if val != "updated" {
			t.Errorf("期望值为'updated'，实际为'%s'", val)
		}
	})

	t.Run("键已过期时失败", func(t *testing.T) {
		expireKey := "cas_expire_key"
		if err := kv.Put(ctx, expireKey, "to_expire", time.Millisecond*10); err != nil {
			t.Fatalf("Put失败: %v", err)
		}
		time.Sleep(time.Millisecond * 20)

		ok, err := kv.CompareAndSwap(ctx, expireKey, "to_expire", "new")
		if err != os.ErrNotExist {
			t.Errorf("期望错误为os.ErrNotExist，实际为%v", err)
		}
		if ok {
			t.Error("CompareAndSwap返回true，但键已过期")
		}
	})
}

// TestList 测试List方法
func TestList(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()

	// 准备测试数据
	keys := []string{
		"prefix_a", "prefix_b", "prefix_c",
		"other_x", "prefix_expired",
	}
	for _, k := range keys {
		ttl := -1
		if k == "prefix_expired" {
			ttl = int(time.Millisecond * 10) // 过期键
		}
		if err := kv.Put(ctx, k, k+"_val", time.Duration(ttl)); err != nil {
			t.Fatalf("Put %s 失败: %v", k, err)
		}
	}
	time.Sleep(time.Millisecond * 20) // 等待过期键过期

	t.Run("查询存在前缀的键", func(t *testing.T) {
		result, err := kv.List(ctx, "prefix_")
		if err != nil {
			t.Fatalf("List失败: %v", err)
		}

		// 期望结果: prefix_a, prefix_b, prefix_c (排除过期的prefix_expired)
		if len(result) != 3 {
			t.Fatalf("期望返回3个结果，实际返回%d个", len(result))
		}
		expected := map[string]string{
			"prefix_a": "prefix_a_val",
			"prefix_b": "prefix_b_val",
			"prefix_c": "prefix_c_val",
		}
		for k, v := range expected {
			if result[k] != v {
				t.Errorf("键%s期望值为'%s'，实际为'%s'", k, v, result[k])
			}
		}
	})

	t.Run("查询不存在的前缀", func(t *testing.T) {
		result, err := kv.List(ctx, "none_")
		if err != nil {
			t.Fatalf("List失败: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("期望返回0个结果，实际返回%d个", len(result))
		}
	})
}

// TestListPage 测试ListPage方法
func TestListPage(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}
	ctx := context.Background()

	// 插入10个带相同前缀的键
	prefix := "page_"
	for i := 0; i < 10; i++ {
		key := prefix + string(rune('a'+i))
		// 间隔写入以确保CreateAt有差异
		time.Sleep(time.Millisecond * 10)
		if err := kv.Put(ctx, key, key+"_val", -1); err != nil {
			t.Fatalf("Put %s 失败: %v", key, err)
		}
	}

	t.Run("第一页数据", func(t *testing.T) {
		result, err := kv.ListPage(ctx, prefix, 0, 3)
		if err != nil {
			t.Fatalf("ListPage失败: %v", err)
		}
		if len(result) != 3 {
			t.Errorf("期望3条数据，实际%d条", len(result))
		}
		// 验证排序（按CreateAt升序，应为page_a, page_b, page_c）
		expected := []string{"page_a", "page_b", "page_c"}
		i := 0
		for k := range result {
			if k != expected[i] {
				t.Errorf("第%d个键期望为%s，实际为%s", i, expected[i], k)
			}
			i++
		}
	})

	t.Run("中间页数据", func(t *testing.T) {
		result, err := kv.ListPage(ctx, prefix, 2, 3)
		if err != nil {
			t.Fatalf("ListPage失败: %v", err)
		}
		if len(result) != 3 {
			t.Errorf("期望3条数据，实际%d条", len(result))
		}
		// 应为page_g, page_h, page_i（0: a-c, 1: d-f, 2: g-i）
		expected := []string{"page_g", "page_h", "page_i"}
		i := 0
		for k := range result {
			if k != expected[i] {
				t.Errorf("第%d个键期望为%s，实际为%s", i, expected[i], k)
			}
			i++
		}
	})

	t.Run("超出范围的页", func(t *testing.T) {
		result, err := kv.ListPage(ctx, prefix, 10, 3)
		if err != nil {
			t.Fatalf("ListPage失败: %v", err)
		}
		if len(result) != 0 {
			t.Errorf("期望0条数据，实际%d条", len(result))
		}
	})
}

// TestClose 测试Close方法
func TestClose(t *testing.T) {
	t.Run("无持久化存储关闭", func(t *testing.T) {
		kv, _ := NewMemory("")
		if err := kv.Close(); err != nil {
			t.Errorf("关闭无存储实例失败: %v", err)
		}
	})

	t.Run("有持久化存储关闭", func(t *testing.T) {
		path := tempFilePath(t)
		kv, err := NewMemory(path)
		if err != nil {
			t.Fatalf("创建实例失败: %v", err)
		}

		// 写入数据
		keys := []string{"close_1", "close_2", "close_expired"}
		for _, k := range keys {
			ttl := -1
			if k == "close_expired" {
				ttl = int(time.Millisecond * 10)
			}
			if err := kv.Put(context.Background(), k, k+"_val", time.Duration(ttl)); err != nil {
				t.Fatalf("Put %s 失败: %v", k, err)
			}
		}
		time.Sleep(time.Millisecond * 20) // 等待过期

		// 关闭并持久化
		if err := kv.Close(); err != nil {
			t.Fatalf("关闭失败: %v", err)
		}

		// 验证持久化结果
		_, err = os.ReadFile(path)
		if err != nil {
			t.Fatalf("读取持久化文件失败: %v", err)
		}

		// 重新加载验证
		newKV, err := NewMemory(path)
		if err != nil {
			t.Fatalf("重新加载失败: %v", err)
		}

		// 过期键不应被持久化
		_, err = newKV.Get(context.Background(), "close_expired")
		if !errors.Is(err, os.ErrNotExist) {
			t.Error("过期键不应被持久化，但能被获取到")
		}

		// 正常键应被持久化
		val1, _ := newKV.Get(context.Background(), "close_1")
		if val1 != "close_1_val" {
			t.Errorf("close_1期望值为'close_1_val'，实际为'%s'", val1)
		}
	})
}

// TestContextCancel 测试上下文取消的情况
func TestContextCancel(t *testing.T) {
	kv, err := NewMemory("")
	if err != nil {
		t.Fatalf("创建实例失败: %v", err)
	}

	// 插入一些数据
	for i := 0; i < 5; i++ {
		key := "ctx_key_" + string(rune('a'+i))
		if err := kv.Put(context.Background(), key, key+"_val", -1); err != nil {
			t.Fatalf("Put %s 失败: %v", key, err)
		}
	}

	t.Run("List时取消上下文", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		_, err := kv.List(ctx, "ctx_key_")
		if !errors.Is(err, context.Canceled) {
			t.Errorf("期望错误为context.Canceled，实际为%v", err)
		}
	})

	t.Run("ListPage时取消上下文", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		_, err := kv.ListPage(ctx, "ctx_key_", 0, 10)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("期望错误为context.Canceled，实际为%v", err)
		}
	})
}
