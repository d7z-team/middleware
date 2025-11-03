package lock

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// EtcdLocker 基于etcd的分布式锁实现
// 适用于跨进程、跨机器的分布式环境下的同步
type EtcdLocker struct {
	client *clientv3.Client
	prefix string // 锁的前缀路径，用于区分不同的锁命名空间
}

// NewEtcdLocker 创建一个新的etcd分布式锁实例
// client: etcd客户端实例
// prefix: 锁的前缀路径，如"/myapp/locks/"
func NewEtcdLocker(client *clientv3.Client, prefix string) *EtcdLocker {
	if prefix == "" {
		prefix = "/locks/"
	} else if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	return &EtcdLocker{
		client: client,
		prefix: prefix,
	}
}

// getLockPath 生成锁的完整路径
func (e *EtcdLocker) getLockPath(id string) string {
	return fmt.Sprintf("%s%s", e.prefix, id)
}

// TryLock 尝试获取分布式锁，非阻塞
// 如果成功获取锁，返回解锁函数；否则返回nil
func (e *EtcdLocker) TryLock(ctx context.Context, id string) func() {
	// 创建会话，设置TTL为60秒，确保锁会自动释放
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil
	}

	// 创建分布式锁
	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	// 尝试获取锁，非阻塞模式
	if err := mutex.TryLock(ctx); err != nil {
		_ = session.Close()
		return nil
	}

	// 返回解锁函数
	return func() {
		// 解锁
		_ = mutex.Unlock(ctx)
		// 关闭会话
		_ = session.Close()
	}
}

// Lock 阻塞直到获取分布式锁或上下文被取消
// 如果成功获取锁，返回解锁函数；否则返回nil
func (e *EtcdLocker) Lock(ctx context.Context, id string) func() {
	// 创建会话，设置TTL为60秒
	session, err := concurrency.NewSession(e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(60))
	if err != nil {
		return nil
	}

	// 创建分布式锁
	mutex := concurrency.NewMutex(session, e.getLockPath(id))

	// 阻塞获取锁
	if err := mutex.Lock(ctx); err != nil {
		_ = session.Close()
		return nil
	}

	// 返回解锁函数
	return func() {
		// 解锁
		_ = mutex.Unlock(ctx)
		// 关闭会话
		_ = session.Close()
	}
}
