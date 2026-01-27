package kv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Etcd is an etcd-based KV storage implementation.
type Etcd struct {
	client *clientv3.Client
	prefix string // key prefix
}

// NewEtcd creates a new etcd KV instance.
func NewEtcd(client *clientv3.Client, prefix string) *Etcd {
	return &Etcd{
		client: client,
		prefix: strings.Trim(prefix, "/") + "/",
	}
}

// Child creates a child KV with appended path.
func (e *Etcd) Child(paths ...string) KV {
	if len(paths) == 0 {
		return e
	}
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		keys = append(keys, path)
	}
	if len(keys) == 0 {
		return e
	}
	return NewEtcd(e.client, e.prefix+strings.Join(keys, "/")+"/")
}

// Count counts the number of keys matching the prefix.
func (e *Etcd) Count(ctx context.Context) (int64, error) {
	resp, err := e.client.Get(ctx, e.prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return 0, fmt.Errorf("count keys failed: %w", err)
	}
	count := int64(0)
	prefixLen := len(e.prefix)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, e.prefix) {
			continue
		}
		relativeKey := key[prefixLen:]
		if !strings.Contains(relativeKey, "/") {
			count++
		}
	}
	return count, nil
}

func (e *Etcd) validateKey(key string) error {
	if strings.Contains(key, "/") {
		return fmt.Errorf("key cannot contain '/': %s", key)
	}
	return nil
}

func (e *Etcd) buildKey(key string) (string, error) {
	if err := e.validateKey(key); err != nil {
		return "", err
	}
	key = strings.TrimPrefix(key, "/")
	return e.prefix + key, nil
}

func (e *Etcd) extractKey(fullKey string) string {
	return strings.TrimPrefix(fullKey, e.prefix)
}

// Put stores a key-value pair. If ttl is not TTLKeep, it sets a lease.
func (e *Etcd) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	fullKey, err := e.buildKey(key)
	if err != nil {
		return err
	}

	var opts []clientv3.OpOption

	if ttl != TTLKeep {
		ttlSeconds := int64(ttl / time.Second)
		if ttlSeconds < 1 {
			ttlSeconds = 1
		}
		resp, err := e.client.Grant(ctx, ttlSeconds)
		if err != nil {
			return fmt.Errorf("create lease failed: %w", err)
		}
		defer func() {
			if err != nil {
				_, _ = e.client.Revoke(context.Background(), resp.ID)
			}
		}()
		opts = append(opts, clientv3.WithLease(resp.ID))
	}

	_, err = e.client.Put(ctx, fullKey, value, opts...)
	if err != nil {
		return fmt.Errorf("put key %s failed: %w", fullKey, err)
	}
	return nil
}

// Get retrieves the value for a key. Returns ErrKeyNotFound if not found.
func (e *Etcd) Get(ctx context.Context, key string) (string, error) {
	fullKey, err := e.buildKey(key)
	if err != nil {
		return "", err
	}

	resp, err := e.client.Get(ctx, fullKey, clientv3.WithLimit(1))
	if err != nil {
		return "", fmt.Errorf("get key %s failed: %w", fullKey, err)
	}

	if resp.Count == 0 {
		return "", errors.Join(ErrKeyNotFound, fmt.Errorf("key %s not found", key))
	}

	return string(resp.Kvs[0].Value), nil
}

// Delete removes a key.
func (e *Etcd) Delete(ctx context.Context, key string) (bool, error) {
	fullKey, err := e.buildKey(key)
	if err != nil {
		return false, err
	}

	r, err := e.client.Delete(ctx, fullKey)
	if err != nil {
		return false, fmt.Errorf("delete key %s failed: %w", fullKey, err)
	}
	return r.Deleted > 0, nil
}

// PutIfNotExists sets the value only if the key does not exist.
func (e *Etcd) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	fullKey, err := e.buildKey(key)
	if err != nil {
		return false, err
	}

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

// CompareAndSwap updates the value if it matches the old value.
func (e *Etcd) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	fullKey, err := e.buildKey(key)
	if err != nil {
		return false, err
	}

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

	if len(txnResp.Responses) > 0 {
		getResp := txnResp.Responses[0].GetResponseRange()
		if getResp != nil && getResp.Count == 0 {
			return false, errors.Join(ErrKeyNotFound, fmt.Errorf("key %s not found", key))
		}
	}

	return false, nil
}

// CursorList implements cursor-based pagination.
func (e *Etcd) CursorList(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := &ListOptions{}
	if options != nil {
		opts = options
	}
	if opts.Limit == 0 {
		opts.Limit = 1000
	}

	etcdOpts := []clientv3.OpOption{
		clientv3.WithKeysOnly(),
		clientv3.WithLimit(opts.Limit + 1),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}

	key := e.prefix
	if opts.Cursor != "" {
		etcdOpts = append(etcdOpts, clientv3.WithFromKey())
		key += opts.Cursor
	} else {
		etcdOpts = append(etcdOpts, clientv3.WithPrefix())
	}

	resp, err := e.client.Get(ctx, key, etcdOpts...)
	if err != nil {
		return nil, fmt.Errorf("list keys failed: %w", err)
	}

	result := &ListResponse{
		Keys: make([]string, 0, len(resp.Kvs)),
	}

	count := int64(0)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, e.prefix) {
			break
		}

		if count >= opts.Limit {
			result.HasMore = true
			result.Cursor = e.extractKey(key)
			break
		}
		result.Keys = append(result.Keys, e.extractKey(key))
		count++
	}

	return result, nil
}
