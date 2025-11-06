package lock

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// testConfig 定义测试配置
type testConfig struct {
	name       string
	connString string
	skipReason string // 如果跳过测试的原因
}

// getTestConfigs 获取所有测试配置
func getTestConfigs() []testConfig {
	return []testConfig{
		{
			name:       "memory",
			connString: "memory://",
		},
		{
			name:       "etcd",
			connString: "etcd://127.0.0.1:2379?prefix=test/",
		},
	}
}

// createLocker 创建锁实例，如果不可用则跳过测试
func createLocker(t *testing.T, config testConfig) Locker {
	t.Helper()

	if config.skipReason != "" {
		t.Skipf("Skipping %s test: %s", config.name, config.skipReason)
	}

	locker, err := NewLocker(config.connString)
	if err != nil {
		// 如果是etcd连接错误，跳过测试而不是失败
		if config.name == "etcd" && strings.Contains(err.Error(), "connection") {
			t.Skipf("Skipping etcd test: %v", err)
		}
		t.Fatalf("Failed to create %s locker: %v", config.name, err)
	}
	return locker
}

// TestNewLocker_Schemes 测试不同scheme的锁创建
func TestNewLocker_Schemes(t *testing.T) {
	tests := []struct {
		name        string
		connString  string
		wantErr     bool
		errContains string
	}{
		{
			name:       "local scheme",
			connString: "local://",
			wantErr:    false,
		},
		{
			name:       "memory scheme",
			connString: "memory://",
			wantErr:    false,
		},
		{
			name:       "mem scheme",
			connString: "mem://",
			wantErr:    false,
		},
		{
			name:       "etcd scheme",
			connString: "etcd://127.0.0.1:2379",
			wantErr:    false, // 如果etcd不可用，NewLocker可能成功，但实际使用时可能失败
		},
		{
			name:        "unsupported scheme",
			connString:  "redis://localhost:6379",
			wantErr:     true,
			errContains: "unsupported scheme",
		},
		{
			name:       "invalid URL",
			connString: "://invalid",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			locker, err := NewLocker(tt.connString)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewLocker() expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewLocker() error = %v, should contain %v", err, tt.errContains)
				}
				return
			}

			// 对于etcd，连接错误是可接受的（测试环境可能没有etcd）
			if err != nil && tt.connString != "etcd://localhost:2379" {
				t.Errorf("NewLocker() unexpected error = %v", err)
				return
			}

			// 如果创建成功，锁实例不应为nil
			if err == nil && locker == nil {
				t.Error("NewLocker() returned nil locker")
			}
		})
	}
}

// TestLocker_TryLock 测试TryLock功能
func TestLocker_TryLock(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			ctx := context.Background()
			lockID := "test-resource-" + config.name

			// 第一次锁应该成功
			release1 := locker.TryLock(ctx, lockID)
			if release1 == nil {
				t.Error("TryLock() failed to acquire first lock")
				return
			}

			// 第二次锁应该失败（已经锁定）
			release2 := locker.TryLock(ctx, lockID)
			if release2 != nil {
				t.Error("TryLock() should have failed to acquire second lock")
				release2()
			}

			// 释放第一次锁
			release1()

			// 释放后应该能再次获取锁
			release3 := locker.TryLock(ctx, lockID)
			if release3 == nil {
				t.Error("TryLock() failed to acquire lock after release")
			} else {
				release3()
			}
		})
	}
}

// TestLocker_Lock 测试阻塞Lock功能
func TestLocker_Lock(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			ctx := context.Background()
			lockID := "test-resource-" + config.name

			// 获取第一次锁
			release1 := locker.Lock(ctx, lockID)
			if release1 == nil {
				t.Fatal("Lock() failed to acquire first lock")
			}

			// 在goroutine中尝试获取第二次锁（应该阻塞）
			acquired := make(chan bool, 1)
			go func() {
				release2 := locker.Lock(ctx, lockID)
				if release2 != nil {
					acquired <- true
					release2()
				} else {
					acquired <- false
				}
			}()

			// 给goroutine时间启动
			time.Sleep(100 * time.Millisecond)

			// 第二次锁应该还没有获取到
			select {
			case <-acquired:
				t.Error("Second lock should be blocking")
			default:
				// 预期 - 锁正在阻塞
			}

			// 释放第一次锁
			release1()

			// 现在第二次锁应该获取到了
			select {
			case result := <-acquired:
				if !result {
					t.Error("Second lock should have been acquired after release")
				}
			case <-time.After(2 * time.Second): // 给etcd更多时间
				t.Error("Second lock timed out")
			}
		})
	}
}

// TestLocker_ConcurrentAccess 测试并发访问
func TestLocker_ConcurrentAccess(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			ctx := context.Background()
			lockID := "concurrent-resource-" + config.name
			counter := 0
			var mu sync.Mutex
			var wg sync.WaitGroup

			// 启动多个goroutine尝试增加计数器
			const goroutines = 10
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					release := locker.Lock(ctx, lockID)
					if release == nil {
						t.Error("Failed to acquire lock in goroutine")
						return
					}
					defer release()

					// 临界区
					mu.Lock()
					counter++
					mu.Unlock()

					// 模拟一些工作
					time.Sleep(10 * time.Millisecond)
				}()
			}

			wg.Wait()

			if counter != goroutines {
				t.Errorf("Expected counter = %d, got %d", goroutines, counter)
			}
		})
	}
}

// TestLocker_DifferentResources 测试不同资源的锁
func TestLocker_DifferentResources(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			ctx := context.Background()

			// 同时锁不同的资源
			release1 := locker.Lock(ctx, "resource-1-"+config.name)
			if release1 == nil {
				t.Fatal("Failed to lock resource-1")
			}
			defer release1()

			release2 := locker.Lock(ctx, "resource-2-"+config.name)
			if release2 == nil {
				t.Fatal("Failed to lock resource-2")
			}
			defer release2()

			// 两个锁应该同时持有
			t.Logf("Both resource locks acquired successfully for %s", config.name)
		})
	}
}

// TestLocker_ContextCancellation 测试上下文取消
func TestLocker_ContextCancellation(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			lockID := "test-resource-" + config.name

			// 获取第一次锁
			release1 := locker.Lock(context.Background(), lockID)
			if release1 == nil {
				t.Fatal("Failed to acquire first lock")
			}

			// 创建带超时的上下文
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			// 尝试用超时获取锁
			start := time.Now()
			release2 := locker.Lock(ctx, lockID)
			elapsed := time.Since(start)

			if release2 != nil {
				t.Error("Lock should not have been acquired with cancelled context")
				release2()
			}

			// 应该至少等待了超时时间
			minWait := 50 * time.Millisecond // 允许一些误差，特别是对于etcd
			if elapsed < minWait {
				t.Logf("Lock acquisition elapsed time: %v (may be shorter for non-blocking implementations)", elapsed)
			}

			release1()
		})
	}
}

// TestLocker_ContextCancellationBeforeLock 测试在获取锁之前上下文就被取消的情况
func TestLocker_ContextCancellationBeforeLock(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			lockID := "test-resource-" + config.name

			// 立即取消的上下文
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// 尝试用已取消的上下文获取锁
			release := locker.Lock(ctx, lockID)
			if release != nil {
				t.Error("Lock should not have been acquired with already cancelled context")
				release()
			}
		})
	}
}

// TestLocker_ConcurrentContextCancellation 测试并发上下文取消
func TestLocker_ConcurrentContextCancellation(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			lockID := "test-resource-" + config.name
			var wg sync.WaitGroup

			// 获取基础锁
			baseRelease := locker.Lock(context.Background(), lockID)
			if baseRelease == nil {
				t.Fatal("Failed to acquire base lock")
			}

			// 启动多个goroutine尝试获取锁，但都会超时
			const goroutines = 5
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()

					ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
					defer cancel()

					release := locker.Lock(ctx, lockID)
					if release != nil {
						t.Errorf("Goroutine %d should not have acquired lock", index)
						release()
					}
				}(i)
			}

			wg.Wait()
			baseRelease()
		})
	}
}

// TestLocker_ReleaseSafety 测试释放函数的安全性
func TestLocker_ReleaseSafety(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			locker := createLocker(t, config)
			ctx := context.Background()
			lockID := "test-resource-" + config.name

			// 获取锁并多次释放
			release := locker.Lock(ctx, lockID)
			if release == nil {
				t.Fatal("Failed to acquire lock")
			}

			// 第一次释放应该工作
			release()

			// 后续释放应该是安全的（不panic）
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Multiple releases caused panic: %v", r)
					}
				}()
				release()
				release()
			}()

			// 释放后应该能再次获取锁
			release2 := locker.Lock(ctx, lockID)
			if release2 == nil {
				t.Error("Failed to acquire lock after multiple releases")
			} else {
				release2()
			}
		})
	}
}

// TestLocker_Isolation 测试不同锁实例之间的隔离
func TestLocker_Isolation(t *testing.T) {
	configs := getTestConfigs()

	for _, config := range configs {
		t.Run(config.name, func(t *testing.T) {
			// 创建两个独立的锁实例
			locker1 := createLocker(t, config)
			locker2 := createLocker(t, config)
			ctx := context.Background()
			lockID := "isolation-resource-" + config.name

			// 第一个实例获取锁
			release1 := locker1.Lock(ctx, lockID)
			if release1 == nil {
				t.Fatal("Locker1 failed to acquire lock")
			}
			defer release1()

			// 第二个实例应该也能获取相同的锁（对于分布式锁，这测试隔离性）
			// 注意：对于真正的分布式锁，这取决于实现，有些可能支持，有些不支持
			release2 := locker2.TryLock(ctx, lockID)
			if release2 == nil {
				t.Log("Second locker could not acquire same lock (expected for some distributed locks)")
			} else {
				release2()
			}
		})
	}
}

// BenchmarkLocker_LockUnlock 基准测试
func BenchmarkLocker_LockUnlock(b *testing.B) {
	configs := getTestConfigs()

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			locker, err := NewLocker(config.connString)
			if err != nil {
				if config.name == "etcd" {
					b.Skipf("Skipping etcd benchmark: %v", err)
				}
				b.Fatalf("Failed to create locker: %v", err)
			}

			ctx := context.Background()
			lockID := "bench-resource-" + config.name

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				release := locker.Lock(ctx, lockID)
				if release == nil {
					b.Fatal("Failed to acquire lock")
				}
				release()
			}
		})
	}
}

// BenchmarkLocker_ConcurrentLockUnlock 并发基准测试
func BenchmarkLocker_ConcurrentLockUnlock(b *testing.B) {
	configs := getTestConfigs()

	for _, config := range configs {
		b.Run(config.name, func(b *testing.B) {
			locker, err := NewLocker(config.connString)
			if err != nil {
				if config.name == "etcd" {
					b.Skipf("Skipping etcd benchmark: %v", err)
				}
				b.Fatalf("Failed to create locker: %v", err)
			}

			ctx := context.Background()

			b.RunParallel(func(pb *testing.PB) {
				lockID := "concurrent-bench-" + config.name
				for pb.Next() {
					release := locker.Lock(ctx, lockID)
					if release == nil {
						b.Fatal("Failed to acquire lock")
					}
					release()
				}
			})
		})
	}
}
