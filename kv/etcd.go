package kv

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Etcd 是 etcd 配置存储的实现，适配KV接口
type Etcd struct {
	client *clientv3.Client
	prefix string
}

// NewEtcd 创建一个新的 etcd 配置存储实例
func NewEtcd(client *clientv3.Client, prefix string) *Etcd {
	if prefix == "" {
		prefix = "/kv/"
	} else if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return &Etcd{
		client: client,
		prefix: prefix,
	}
}

func (e *Etcd) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	prefix = e.prefix + "/" + prefix
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend), // 原生排序
	}
	if pageIndex > 0 {
		// 先获取前 (pageIndex) 页的最后一个 key，作为当前页的起始点
		// 总偏移量 = pageIndex * pageLength
		offset := pageIndex * uint64(pageSize)
		resp, err := e.client.Get(ctx, prefix, append(opts, clientv3.WithLimit(int64(offset)))...)
		if err != nil {
			return nil, fmt.Errorf("failed to get offset for page %d: %w", pageIndex, err)
		}
		if len(resp.Kvs) < int(offset) {
			// 总数据量不足，返回空结果
			return map[string]string{}, nil
		}
		opts = append(opts, clientv3.WithFromKey()) // 从 startKey 之后开始查询
	}

	opts = append(opts, clientv3.WithLimit(int64(pageSize)))

	resp, err := e.client.Get(ctx, prefix, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list keys with prefix %s: %w", prefix, err)
	}

	result := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

// Put 存储键值对，可以设置过期时间
func (e *Etcd) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	key = e.prefix + "/" + key
	var opts []clientv3.OpOption

	// 如果 TTL 不是 TTLKeep，则创建租约
	if ttl != TTLKeep && ttl > 0 {
		// 创建租约
		resp, err := e.client.Grant(ctx, int64(ttl.Seconds()))
		if err != nil {
			return fmt.Errorf("failed to create lease: %w", err)
		}
		opts = append(opts, clientv3.WithLease(resp.ID))
	}

	// 执行 PUT 操作
	_, err := e.client.Put(ctx, key, value, opts...)
	if err != nil {
		return fmt.Errorf("failed to put key %s: %w", key, err)
	}
	return nil
}

// Get 获取指定键的值
func (e *Etcd) Get(ctx context.Context, key string) (string, error) {
	key = e.prefix + "/" + key
	resp, err := e.client.Get(ctx, key, clientv3.WithLimit(1))
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if resp.Count == 0 {
		return "", errors.Join(ErrKeyNotFound, fmt.Errorf("key %s not found", key))
	}

	return string(resp.Kvs[0].Value), nil
}

// Delete 删除指定的键
func (e *Etcd) Delete(ctx context.Context, key string) (bool, error) {
	key = e.prefix + "/" + key
	r, err := e.client.Delete(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return r.Deleted > 0, nil
}

// PutIfNotExists 仅在键不存在时设置值
func (e *Etcd) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	key = e.prefix + "/" + key
	// 设置条件：键不存在
	cmp := clientv3.Compare(clientv3.Version(key), "=", 0)

	// 准备PUT操作
	putOp := clientv3.OpPut(key, value)

	// 如果需要设置TTL
	if ttl != TTLKeep && ttl > 0 {
		resp, err := e.client.Grant(ctx, int64(ttl.Seconds()))
		if err != nil {
			return false, fmt.Errorf("failed to create lease: %w", err)
		}
		putOp = clientv3.OpPut(key, value, clientv3.WithLease(resp.ID))
	}

	// 执行事务
	txnResp, err := e.client.Txn(ctx).If(cmp).Then(putOp).Commit()
	if err != nil {
		return false, fmt.Errorf("failed to execute put if not exists: %w", err)
	}

	// 返回是否成功设置了值
	return txnResp.Succeeded, nil
}

// CompareAndSwap 当当前值等于oldValue时，将其更新为newValue
func (e *Etcd) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	key = e.prefix + "/" + key
	// 获取当前键的版本
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	// 键不存在的情况
	if resp.Count == 0 {
		return false, fmt.Errorf("key %s not found", key)
	}

	// 比较当前值与旧值
	currentValue := string(resp.Kvs[0].Value)
	if currentValue != oldValue {
		return false, nil
	}

	// 比较版本以确保原子性
	cmp := clientv3.Compare(clientv3.Version(key), "=", resp.Kvs[0].Version)
	putOp := clientv3.OpPut(key, newValue)

	// 执行事务
	txnResp, err := e.client.Txn(ctx).If(cmp).Then(putOp).Commit()
	if err != nil {
		return false, fmt.Errorf("failed to execute compare and swap: %w", err)
	}

	return txnResp.Succeeded, nil
}

func (e *Etcd) List(ctx context.Context, prefix string) (map[string]string, error) {
	prefix = e.prefix + "/" + prefix
	// 使用 WithPrefix 选项获取前缀匹配的所有键
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list keys with prefix %s: %w", prefix, err)
	}

	result := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		result[string(kv.Key)] = string(kv.Value)
	}

	return result, nil
}

func (e *Etcd) Spliter() string {
	return "/"
}

// Close 关闭 etcd 客户端连接
func (e *Etcd) Close() error {
	return nil
}
