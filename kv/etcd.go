package kv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Etcd 是 etcd 配置存储的实现，适配KV接口
type Etcd struct {
	client *clientv3.Client
	prefix string // 已确保以"/"结尾
	closed atomic.Bool
}

// NewEtcd 创建一个新的 etcd 配置存储实例
func NewEtcd(client *clientv3.Client, prefix string) *Etcd {
	if prefix == "" {
		prefix = "/kv/"
	} else if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	etcd := &Etcd{
		client: client,
		prefix: prefix,
	}
	etcd.closed.Store(false)
	return etcd
}

// 检查客户端是否已关闭
func (e *Etcd) checkClosed() error {
	if e.closed.Load() {
		return ErrClosed
	}
	return nil
}

// 辅助函数：拼接键（避免双斜杠）
func (e *Etcd) buildKey(key string) string {
	// 移除key开头可能的"/"，避免拼接后多斜杠
	key = strings.TrimPrefix(key, "/")
	return e.prefix + key
}

// 辅助函数：从完整键中剥离实例前缀，返回相对键
func (e *Etcd) stripPrefix(fullKey string) string {
	return strings.TrimPrefix(fullKey, e.prefix)
}

// Put 存储键值对（添加关闭检查）
func (e *Etcd) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := e.checkClosed(); err != nil {
		return err
	}

	fullKey := e.buildKey(key)
	var opts []clientv3.OpOption

	if ttl != TTLKeep {
		// 精确处理TTL，支持毫秒级，但etcd最小TTL为1秒
		ttlSeconds := int64(ttl / time.Second)
		if ttlSeconds < 1 {
			ttlSeconds = 1 // etcd 最小TTL为1秒
		}
		resp, err := e.client.Grant(ctx, ttlSeconds)
		if err != nil {
			return fmt.Errorf("create lease failed: %w", err)
		}
		opts = append(opts, clientv3.WithLease(resp.ID))
	}

	_, err := e.client.Put(ctx, fullKey, value, opts...)
	if err != nil {
		return fmt.Errorf("put key %s failed: %w", fullKey, err)
	}
	return nil
}

// Get 获取指定键的值（添加关闭检查）
func (e *Etcd) Get(ctx context.Context, key string) (string, error) {
	if err := e.checkClosed(); err != nil {
		return "", err
	}

	fullKey := e.buildKey(key)
	resp, err := e.client.Get(ctx, fullKey, clientv3.WithLimit(1))
	if err != nil {
		return "", fmt.Errorf("get key %s failed: %w", fullKey, err)
	}

	if resp.Count == 0 {
		// 使用 errors.Join 保持错误链，让测试用例能通过 ErrorIs 判断
		return "", errors.Join(ErrKeyNotFound, fmt.Errorf("key %s not found", key))
	}

	return string(resp.Kvs[0].Value), nil
}

// Delete 删除指定的键（添加关闭检查）
func (e *Etcd) Delete(ctx context.Context, key string) (bool, error) {
	if err := e.checkClosed(); err != nil {
		return false, err
	}

	fullKey := e.buildKey(key)
	r, err := e.client.Delete(ctx, fullKey)
	if err != nil {
		return false, fmt.Errorf("delete key %s failed: %w", fullKey, err)
	}
	return r.Deleted > 0, nil
}

// PutIfNotExists 仅在键不存在时设置值（添加关闭检查）
func (e *Etcd) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if err := e.checkClosed(); err != nil {
		return false, err
	}

	fullKey := e.buildKey(key)
	cmp := clientv3.Compare(clientv3.Version(fullKey), "=", 0)

	var putOp clientv3.Op
	if ttl != TTLKeep {
		ttlSeconds := int64(ttl / time.Second)
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
		resp, err := e.client.Grant(ctx, ttlSeconds)
		if err != nil {
			return false, fmt.Errorf("create lease failed: %w", err)
		}
		putOp = clientv3.OpPut(fullKey, value, clientv3.WithLease(resp.ID))
	} else {
		putOp = clientv3.OpPut(fullKey, value)
	}

	txnResp, err := e.client.Txn(ctx).If(cmp).Then(putOp).Commit()
	if err != nil {
		return false, fmt.Errorf("put if not exists failed: %w", err)
	}

	return txnResp.Succeeded, nil
}

// CompareAndSwap 原子CAS操作（添加关闭检查）
func (e *Etcd) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	if err := e.checkClosed(); err != nil {
		return false, err
	}

	fullKey := e.buildKey(key)

	// 使用单次事务避免竞争条件
	cmp := clientv3.Compare(clientv3.Value(fullKey), "=", oldValue)
	putOp := clientv3.OpPut(fullKey, newValue)
	getOp := clientv3.OpGet(fullKey)

	txnResp, err := e.client.Txn(ctx).
		If(cmp).
		Then(putOp).
		Else(getOp).
		Commit()
	if err != nil {
		return false, fmt.Errorf("compare and swap failed: %w", err)
	}

	if txnResp.Succeeded {
		return true, nil
	}

	// 如果事务失败，检查键是否存在
	if len(txnResp.Responses) > 0 {
		getResp := txnResp.Responses[0].GetResponseRange()
		if getResp != nil && getResp.Count == 0 {
			return false, errors.Join(ErrKeyNotFound, fmt.Errorf("key %s not found", key))
		}
	}

	return false, nil
}

func (e *Etcd) Spliter() string {
	return "/"
}

// Close 关闭Etcd客户端（添加原子性关闭检查）
func (e *Etcd) Close() error {
	// 使用 CAS 操作确保只关闭一次
	if e.closed.CompareAndSwap(false, true) {
		if e.client != nil {
			return e.client.Close()
		}
	}
	return nil
}

// IsClosed 检查客户端是否已关闭（用于测试或外部检查）
func (e *Etcd) IsClosed() bool {
	return e.closed.Load()
}
