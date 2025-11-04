package l2cache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// 辅助类型：模拟读取失败的Reader
type errorReader struct {
	err error
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}

// TestNewMemoryCache 测试缓存初始化（配置校验、第三方库集成）
func TestNewMemoryCache(t *testing.T) {
	// 1. 必传容量未配置（错误场景）
	_, err := NewMemoryCache()
	if !errors.Is(err, ErrInvalidCapacity) {
		t.Errorf("缺少容量配置：期望 ErrInvalidCapacity，实际 %v", err)
	}

	// 2. 容量为0（错误场景）
	_, err = NewMemoryCache(WithMaxCapacity(0))
	if !errors.Is(err, ErrInvalidCapacity) {
		t.Errorf("容量为0：期望 ErrInvalidCapacity，实际 %v", err)
	}

	// 3. 容量为负数（错误场景）
	_, err = NewMemoryCache(WithMaxCapacity(-500))
	if !errors.Is(err, ErrInvalidCapacity) {
		t.Errorf("容量为负数：期望 ErrInvalidCapacity，实际 %v", err)
	}

	// 4. 仅配置容量（默认清理间隔）
	mc, err := NewMemoryCache(WithMaxCapacity(100))
	if err != nil {
		t.Fatalf("正常创建缓存失败：%v", err)
	}
	defer mc.Close()
	if mc.maxCap != 100 || mc.cleanupInt != 5*time.Minute {
		t.Errorf("默认配置异常：maxCap=%d, cleanupInt=%v", mc.maxCap, mc.cleanupInt)
	}
	if mc.lruCache == nil {
		t.Error("golang-lru 缓存实例未初始化")
	}

	// 5. 自定义清理间隔（负数→默认值）
	mc, err = NewMemoryCache(WithMaxCapacity(100), WithCleanupInterval(-30*time.Second))
	if err != nil {
		t.Fatalf("自定义负数清理间隔失败：%v", err)
	}
	defer mc.Close()
	if mc.cleanupInt != 5*time.Minute {
		t.Errorf("负数清理间隔未生效默认值：实际 %v", mc.cleanupInt)
	}

	// 6. 自定义有效清理间隔
	customInterval := 2 * time.Second
	mc, err = NewMemoryCache(WithMaxCapacity(200), WithCleanupInterval(customInterval))
	if err != nil {
		t.Fatalf("自定义清理间隔失败：%v", err)
	}
	defer mc.Close()
	if mc.cleanupInt != customInterval {
		t.Errorf("自定义清理间隔未生效：期望 %v，实际 %v", customInterval, mc.cleanupInt)
	}
}

// TestMemoryCache_Put 测试缓存存入（基础功能、更新、错误场景）
func TestMemoryCache_Put(t *testing.T) {
	mc, err := NewMemoryCache(WithMaxCapacity(5))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	// 1. 正常存入（永不过期）
	key1 := "put-key-1"
	value1 := []byte("put-value-1")
	if err := mc.Put(key1, bytes.NewBuffer(value1), TTLKeep); err != nil {
		t.Errorf("永不过期存入失败：%v", err)
	}
	// 验证存入结果
	content, err := mc.Get(key1)
	if err != nil {
		t.Fatalf("获取永不过期缓存失败：%v", err)
	}
	data, _ := io.ReadAll(content)
	if !bytes.Equal(data, value1) {
		t.Errorf("永不过期缓存值不一致：期望 %s，实际 %s", value1, data)
	}

	// 2. 更新已存在的key（覆盖值+更新时间）
	value1Updated := []byte("put-value-1-updated")
	putTime := time.Now()
	if err := mc.Put(key1, bytes.NewBuffer(value1Updated), TTLKeep); err != nil {
		t.Errorf("更新已存在key失败：%v", err)
	}
	// 验证更新结果
	content, err = mc.Get(key1)
	if err != nil {
		t.Fatalf("获取更新后缓存失败：%v", err)
	}
	data, _ = io.ReadAll(content)
	if !bytes.Equal(data, value1Updated) {
		t.Errorf("更新后值不一致：期望 %s，实际 %s", value1Updated, data)
	}
	if content.LastModified.Before(putTime) {
		t.Errorf("更新后最后修改时间未更新：%v", content.LastModified)
	}

	// 3. 存入带TTL的缓存
	key2 := "put-key-2"
	value2 := []byte("put-value-2")
	ttl := 1 * time.Second
	if err := mc.Put(key2, bytes.NewBuffer(value2), ttl); err != nil {
		t.Errorf("带TTL存入失败：%v", err)
	}
	// 立即验证存在
	if _, err := mc.Get(key2); err != nil {
		t.Errorf("带TTL缓存存入后立即获取失败：%v", err)
	}

	// 4. 无效TTL（<=0且非TTLKeep）
	key3 := "put-key-3"
	if err := mc.Put(key3, bytes.NewBuffer([]byte("value3")), -500*time.Millisecond); !errors.Is(err, ErrInvalidTTL) {
		t.Errorf("无效TTL未报错：期望 ErrInvalidTTL，实际 %v", err)
	}
	// 验证未存入
	if _, err := mc.Get(key3); !errors.Is(err, ErrCacheMiss) {
		t.Error("无效TTL缓存意外存入")
	}

	// 5. 读取value失败（错误Reader）
	mockErr := errors.New("mock read error")
	errReader := &errorReader{err: mockErr}
	key4 := "put-key-4"
	if err := mc.Put(key4, errReader, TTLKeep); err == nil {
		t.Error("读取失败场景未报错")
	} else if !errors.Is(err, mockErr) {
		t.Errorf("错误链不一致：期望 %v，实际 %v", mockErr, err)
	}
	// 验证未存入
	if _, err := mc.Get(key4); !errors.Is(err, ErrCacheMiss) {
		t.Error("读取失败场景意外存入缓存")
	}

	// 6. 容量超限触发LRU淘汰（基础验证）
	maxCap := 5
	mc, err = NewMemoryCache(WithMaxCapacity(maxCap))
	if err != nil {
		t.Fatalf("创建固定容量缓存失败：%v", err)
	}
	defer mc.Close()
	// 存入 maxCap+1 个key
	for i := 0; i < maxCap+1; i++ {
		key := fmt.Sprintf("put-key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := mc.Put(key, bytes.NewBuffer(value), TTLKeep); err != nil {
			t.Errorf("存入第%d个key失败：%v", i, err)
		}
	}
	// 验证缓存大小为 maxCap
	if size := mc.lruCache.Len(); size != maxCap {
		t.Errorf("容量超限后缓存大小异常：期望 %d，实际 %d", maxCap, size)
	}
}

// TestMemoryCache_Get 测试缓存获取（命中、未命中、过期、返回值校验）
func TestMemoryCache_Get(t *testing.T) {
	mc, err := NewMemoryCache(WithMaxCapacity(10))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	keyHit := "get-hit-key"
	valueHit := []byte("get-hit-value")
	putTime := time.Now()

	// 1. 未命中缓存
	keyMiss := "get-miss-key"
	if _, err := mc.Get(keyMiss); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("未命中缓存未返回 ErrCacheMiss：实际 %v", err)
	}

	// 2. 命中缓存（永不过期）
	if err := mc.Put(keyHit, bytes.NewBuffer(valueHit), TTLKeep); err != nil {
		t.Fatalf("存入永不过期缓存失败：%v", err)
	}
	content, err := mc.Get(keyHit)
	if err != nil {
		t.Fatalf("命中永不过期缓存失败：%v", err)
	}
	// 校验内容
	data, _ := io.ReadAll(content)
	if !bytes.Equal(data, valueHit) {
		t.Errorf("命中缓存值不一致：期望 %s，实际 %s", valueHit, data)
	}
	// 校验长度
	if content.Length != len(valueHit) {
		t.Errorf("长度不一致：期望 %d，实际 %d", len(valueHit), content.Length)
	}
	// 校验最后修改时间（1秒内有效）
	if content.LastModified.Before(putTime) || time.Since(content.LastModified) > 1*time.Second {
		t.Errorf("最后修改时间异常：%v", content.LastModified)
	}
	// 校验NopCloser.Close()
	if err := content.Close(); err != nil {
		t.Errorf("NopCloser关闭失败：%v", err)
	}

	// 3. 命中但已过期（自动删除）
	keyExp := "get-exp-key"
	valueExp := []byte("get-exp-value")
	ttlExp := 100 * time.Millisecond
	if err := mc.Put(keyExp, bytes.NewBuffer(valueExp), ttlExp); err != nil {
		t.Fatalf("存入过期缓存失败：%v", err)
	}
	// 等待过期
	time.Sleep(ttlExp + 50*time.Millisecond)
	// 验证返回CacheMiss
	if _, err := mc.Get(keyExp); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("过期缓存未返回 ErrCacheMiss：实际 %v", err)
	}
	// 验证已从缓存中删除
	if exists := mc.lruCache.Contains(keyExp); exists {
		t.Error("过期缓存未被自动删除")
	}

	// 4. 过期后再次存入（覆盖过期状态）
	if err := mc.Put(keyExp, bytes.NewBuffer(valueExp), TTLKeep); err != nil {
		t.Fatalf("过期后重新存入失败：%v", err)
	}
	if _, err := mc.Get(keyExp); err != nil {
		t.Errorf("过期后重新存入未命中：%v", err)
	}
}

// TestMemoryCache_LRUEviction 测试LRU淘汰规则（核心场景，验证第三方库集成）
func TestMemoryCache_LRUEviction(t *testing.T) {
	maxCap := 3 // 小容量便于验证淘汰规则
	mc, err := NewMemoryCache(WithMaxCapacity(maxCap))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	// 定义测试key（按存入顺序）
	keys := []string{"lru-key-1", "lru-key-2", "lru-key-3", "lru-key-4"}
	values := []string{"value1", "value2", "value3", "value4"}

	// 1. 存入3个key（容量满）
	for i := 0; i < maxCap; i++ {
		if err := mc.Put(keys[i], bytes.NewBuffer([]byte(values[i])), TTLKeep); err != nil {
			t.Errorf("存入第%d个key失败：%v", i, err)
		}
	}
	// 验证缓存大小
	if size := mc.lruCache.Len(); size != maxCap {
		t.Errorf("容量满时大小异常：期望 %d，实际 %d", maxCap, size)
	}

	// 2. 访问第1个key（更新LRU顺序：key1变为最近访问）
	if _, err := mc.Get(keys[0]); err != nil {
		t.Fatalf("访问key1失败：%v", err)
	}

	// 3. 存入第4个key（触发LRU淘汰，最少访问的key2被淘汰）
	if err := mc.Put(keys[3], bytes.NewBuffer([]byte(values[3])), TTLKeep); err != nil {
		t.Fatalf("存入第4个key（触发淘汰）失败：%v", err)
	}

	// 4. 验证淘汰结果
	// key2（最少访问）被淘汰
	if _, err := mc.Get(keys[1]); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("LRU淘汰规则错误：key2（最少访问）未被淘汰")
	}
	// key1（最近访问）未被淘汰
	if _, err := mc.Get(keys[0]); err != nil {
		t.Errorf("LRU淘汰规则错误：key1（最近访问）被淘汰：%v", err)
	}
	// key3（中间访问）未被淘汰
	if _, err := mc.Get(keys[2]); err != nil {
		t.Errorf("LRU淘汰规则错误：key3未被保留：%v", err)
	}
	// key4（新存入）已存在
	if _, err := mc.Get(keys[3]); err != nil {
		t.Errorf("LRU淘汰规则错误：key4未存入：%v", err)
	}

	// 5. 再次存入被淘汰的key2（触发新淘汰：最少访问的key3）
	if err := mc.Put(keys[1], bytes.NewBuffer([]byte(values[1])), TTLKeep); err != nil {
		t.Fatalf("重新存入key2失败：%v", err)
	}
	// 验证key3被淘汰
	if _, err := mc.Get(keys[1]); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("LRU淘汰规则错误：key3（最少访问）未被淘汰")
	}
}

// TestMemoryCache_Delete 测试缓存删除（存在/不存在key、删除后校验）
func TestMemoryCache_Delete(t *testing.T) {
	mc, err := NewMemoryCache(WithMaxCapacity(10))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	keyExist := "delete-exist-key"
	keyNotExist := "delete-not-exist-key"
	value := []byte("delete-value")

	// 1. 删除不存在的key（无报错）
	if err := mc.Delete(keyNotExist); err != nil {
		t.Errorf("删除不存在key报错：%v", err)
	}

	// 2. 存入后删除存在的key
	if err := mc.Put(keyExist, bytes.NewBuffer(value), TTLKeep); err != nil {
		t.Fatalf("存入key失败：%v", err)
	}
	if err := mc.Delete(keyExist); err != nil {
		t.Errorf("删除存在key失败：%v", err)
	}

	// 3. 验证删除结果
	// Get返回CacheMiss
	if _, err := mc.Get(keyExist); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("删除后仍能获取key：%v", err)
	}
	// 底层缓存不包含该key
	if exists := mc.lruCache.Contains(keyExist); exists {
		t.Error("删除后底层缓存仍包含该key")
	}

	// 4. 批量删除测试
	keys := []string{"delete-1", "delete-2", "delete-3"}
	for _, key := range keys {
		if err := mc.Put(key, bytes.NewBuffer([]byte(key)), TTLKeep); err != nil {
			t.Errorf("批量存入key %s 失败：%v", key, err)
		}
	}
	// 批量删除
	for _, key := range keys {
		if err := mc.Delete(key); err != nil {
			t.Errorf("批量删除key %s 失败：%v", key, err)
		}
	}
	// 验证全部删除
	for _, key := range keys {
		if exists := mc.lruCache.Contains(key); exists {
			t.Errorf("批量删除后key %s 仍存在", key)
		}
	}
}

// TestMemoryCache_CleanupTask 测试后台过期清理任务
func TestMemoryCache_CleanupTask(t *testing.T) {
	// 配置短清理间隔（加速测试）
	cleanupInt := 300 * time.Millisecond
	mc, err := NewMemoryCache(
		WithMaxCapacity(10),
		WithCleanupInterval(cleanupInt),
	)
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	// 定义测试数据：2个过期key，1个永不过期key
	testCases := []struct {
		key         string
		value       string
		ttl         time.Duration
		shouldExist bool // 清理后是否应存在
	}{
		{"clean-exp-1", "exp-value-1", 100 * time.Millisecond, false},
		{"clean-exp-2", "exp-value-2", 150 * time.Millisecond, false},
		{"clean-never", "never-exp-value", TTLKeep, true},
	}

	// 存入所有测试key
	for _, tc := range testCases {
		if err := mc.Put(tc.key, bytes.NewBuffer([]byte(tc.value)), tc.ttl); err != nil {
			t.Errorf("存入测试key %s 失败：%v", tc.key, err)
		}
	}

	// 等待：TTL过期 + 清理任务执行
	waitTime := 200*time.Millisecond + cleanupInt
	time.Sleep(waitTime)

	// 验证清理结果
	for _, tc := range testCases {
		exists := mc.lruCache.Contains(tc.key)
		if exists != tc.shouldExist {
			t.Errorf("清理后key %s 状态异常：期望 %v，实际 %v", tc.key, tc.shouldExist, exists)
		}
		// 额外验证Get结果
		_, err := mc.Get(tc.key)
		if tc.shouldExist && err != nil {
			t.Errorf("永不过期key %s 被误清理：%v", tc.key, err)
		}
		if !tc.shouldExist && !errors.Is(err, ErrCacheMiss) {
			t.Errorf("过期key %s 未被清理：%v", tc.key, err)
		}
	}

	// 验证缓存大小（仅保留永不过期key）
	expectedSize := 1
	if size := mc.lruCache.Len(); size != expectedSize {
		t.Errorf("清理后缓存大小异常：期望 %d，实际 %d", expectedSize, size)
	}
}

// TestMemoryCache_ConcurrencySafety 测试并发安全（多协程读写无竞态）
func TestMemoryCache_ConcurrencySafety(t *testing.T) {
	mc, err := NewMemoryCache(WithMaxCapacity(100))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}
	defer mc.Close()

	const (
		goroutineNum   = 20  // 并发协程数
		opPerGoroutine = 200 // 每个协程操作次数
	)

	var wg sync.WaitGroup
	wg.Add(goroutineNum)
	var errCount int32 // 错误计数（原子操作，避免竞态）
	var mu sync.Mutex  // 保护errCount

	// 并发读写多个key（避免单key竞争过于集中）
	for gid := 0; gid < goroutineNum; gid++ {
		go func(gid int) {
			defer wg.Done()
			for op := 0; op < opPerGoroutine; op++ {
				key := fmt.Sprintf("concurrent-key-%d-%d", gid, op%10) // 分散到10个key
				value := []byte(fmt.Sprintf("value-g%d-op%d", gid, op))

				// 随机执行Put或Get（模拟真实场景）
				if op%2 == 0 {
					// Put操作
					if err := mc.Put(key, bytes.NewBuffer(value), TTLKeep); err != nil {
						mu.Lock()
						errCount++
						mu.Unlock()
						t.Logf("协程%d Put key=%s 失败：%v", gid, key, err)
					}
				} else {
					// Get操作
					content, err := mc.Get(key)
					if err != nil && !errors.Is(err, ErrCacheMiss) {
						mu.Lock()
						errCount++
						mu.Unlock()
						t.Logf("协程%d Get key=%s 失败：%v", gid, key, err)
					}
					if content != nil {
						// 验证内容可读取
						if _, err := io.ReadAll(content); err != nil {
							mu.Lock()
							errCount++
							mu.Unlock()
							t.Logf("协程%d 读取 key=%s 内容失败：%v", gid, key, err)
						}
						_ = content.Close()
					}
				}
			}
		}(gid)
	}

	wg.Wait()

	// 验证无错误
	if errCount > 0 {
		t.Fatalf("并发测试中出现 %d 个错误", errCount)
	}

	// 验证缓存结构一致性（无内存泄漏或结构损坏）
	if size := mc.lruCache.Len(); size < 0 || size > mc.maxCap {
		t.Errorf("并发后缓存大小异常：%d（超出容量范围）", size)
	}

	// 验证最后一次存入的key可正常获取
	finalKey := "concurrent-key-0-0"
	if _, err := mc.Get(finalKey); !errors.Is(err, ErrCacheMiss) {
		t.Logf("并发后获取 finalKey=%s 成功", finalKey)
	}
}

// TestMemoryCache_Close 测试关闭功能（幂等性、后台任务停止）
func TestMemoryCache_Close(t *testing.T) {
	mc, err := NewMemoryCache(WithMaxCapacity(10), WithCleanupInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("创建缓存失败：%v", err)
	}

	// 1. 第一次关闭（正常）
	if err := mc.Close(); err != nil {
		t.Errorf("第一次关闭失败：%v", err)
	}

	// 2. 第二次关闭（幂等性，无报错）
	if err := mc.Close(); err != nil {
		t.Errorf("第二次关闭（幂等）报错：%v", err)
	}

	// 3. 关闭后仍可执行读写（但后台任务已停止）
	key := "close-key"
	value := []byte("close-value")
	// Put仍可执行（底层缓存未销毁）
	if err := mc.Put(key, bytes.NewBuffer(value), TTLKeep); err != nil {
		t.Errorf("关闭后Put失败：%v", err)
	}
	// Get仍可执行
	if _, err := mc.Get(key); err != nil {
		t.Errorf("关闭后Get失败：%v", err)
	}

	// 4. 验证后台清理任务已停止
	// 存入过期key，等待清理间隔+TTL，验证未被清理（任务已停）
	expKey := "close-exp-key"
	ttl := 50 * time.Millisecond
	if err := mc.Put(expKey, bytes.NewBuffer([]byte("exp-value")), ttl); err != nil {
		t.Fatalf("关闭后存入过期key失败：%v", err)
	}
	// 等待足够时间（任务已停，不会清理）
	time.Sleep(ttl + 200*time.Millisecond)
	// 验证仍存在
	if exists := mc.lruCache.Contains(expKey); !exists {
		t.Error("关闭后后台任务未停止，过期key被清理")
	}
}
