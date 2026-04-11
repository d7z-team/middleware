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
	resp, err := e.client.Get(ctx, e.prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, fmt.Errorf("count keys failed: %w", err)
	}
	return resp.Count, nil
}

func (e *Etcd) buildKey(key string) (string, error) {
	if strings.Contains(key, "/") {
		return "", ErrInvalidKey
	}
	return e.prefix + key, nil
}

func (e *Etcd) extractKey(fullKey string) string {
	return strings.TrimPrefix(fullKey, e.prefix)
}

func ttlToSeconds(ttl time.Duration) int64 {
	ttlSeconds := int64(ttl / time.Second)
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}
	return ttlSeconds
}

// List returns all key-value pairs matching the prefix.
func (e *Etcd) List(ctx context.Context, prefix string) ([]Pair, error) {
	res, err := e.Scan(ctx, ScanOptions{
		Prefix: prefix,
		Limit:  1000000,
	})
	if err != nil {
		return nil, err
	}
	return res.Pairs, nil
}

// ListCurrent returns key-value pairs at the current level (excluding children).
func (e *Etcd) ListCurrent(ctx context.Context, prefix string) ([]Pair, error) {
	all, err := e.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	filtered := make([]Pair, 0)
	for _, p := range all {
		if isCurrentLevel(p.Key, prefix) {
			filtered = append(filtered, p)
		}
	}
	return filtered, nil
}

// ListPage returns a page of key-value pairs matching the prefix.
func (e *Etcd) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := e.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	start, end := listPageRange(len(all), pageIndex, pageSize)
	return all[start:end], nil
}

// ListCurrentPage returns a page of key-value pairs at the current level (excluding children).
func (e *Etcd) ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := e.ListCurrent(ctx, prefix)
	if err != nil {
		return nil, err
	}
	start, end := listPageRange(len(all), pageIndex, pageSize)
	return all[start:end], nil
}

// ListCurrentCursor implements cursor-based pagination for current level.
func (e *Etcd) ListCurrentCursor(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := normalizeListOptions(options)

	all, err := e.ListCurrent(ctx, "")
	if err != nil {
		return nil, err
	}

	startIndex := listCursorStartIndex(all, opts.Cursor)
	endIndex := startIndex + int(opts.Limit)
	if endIndex > len(all) {
		endIndex = len(all)
	}

	resultPairs := all[startIndex:endIndex]
	var nextCursor string
	hasMore := endIndex < len(all)
	if hasMore {
		nextCursor = resultPairs[len(resultPairs)-1].Key
	}

	return &ListResponse{
		Pairs:   resultPairs,
		Cursor:  nextCursor,
		HasMore: hasMore,
	}, nil
}

func (e *Etcd) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	fullKey, err := e.buildKey(key)
	if err != nil {
		return err
	}

	if ttl == TTLKeep {
		cmp := clientv3.Compare(clientv3.Version(fullKey), ">", 0)
		opPutKeep := clientv3.OpPut(fullKey, value, clientv3.WithIgnoreLease())
		opPutNew := clientv3.OpPut(fullKey, value)
		_, err := e.client.Txn(ctx).If(cmp).Then(opPutKeep).Else(opPutNew).Commit()
		if err != nil {
			return fmt.Errorf("put key %s failed (ttl=keep): %w", fullKey, err)
		}
		return nil
	}

	resp, err := e.client.Grant(ctx, ttlToSeconds(ttl))
	if err != nil {
		return fmt.Errorf("create lease failed: %w", err)
	}

	_, err = e.client.Put(ctx, fullKey, value, clientv3.WithLease(resp.ID))
	if err != nil {
		_, _ = e.client.Revoke(context.Background(), resp.ID)
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

// DeleteAll removes all keys under the current prefix.
func (e *Etcd) DeleteAll(ctx context.Context) error {
	_, err := e.client.Delete(ctx, e.prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("delete all keys failed: %w", err)
	}
	return nil
}

// PutIfNotExists sets the value only if the key does not exist.
func (e *Etcd) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if invalidTTL(ttl) {
		return false, ErrInvalidTTL
	}
	fullKey, err := e.buildKey(key)
	if err != nil {
		return false, err
	}

	cmp := clientv3.Compare(clientv3.Version(fullKey), "=", 0)

	var putOp clientv3.Op
	var leaseID clientv3.LeaseID
	if ttl != TTLKeep {
		resp, err := e.client.Grant(ctx, ttlToSeconds(ttl))
		if err != nil {
			return false, fmt.Errorf("create lease failed: %w", err)
		}
		leaseID = resp.ID
		putOp = clientv3.OpPut(fullKey, value, clientv3.WithLease(leaseID))
	} else {
		putOp = clientv3.OpPut(fullKey, value)
	}

	txnResp, err := e.client.Txn(ctx).If(cmp).Then(putOp).Commit()
	if err != nil {
		if leaseID != 0 {
			_, _ = e.client.Revoke(context.Background(), leaseID)
		}
		return false, fmt.Errorf("put if not exists failed: %w", err)
	}
	if leaseID != 0 && !txnResp.Succeeded {
		_, _ = e.client.Revoke(context.Background(), leaseID)
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
			return false, nil
		}
	}

	return false, nil
}

// ListCursor implements cursor-based pagination.
func (e *Etcd) ListCursor(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := normalizeListOptions(options)

	res, err := e.Scan(ctx, ScanOptions{
		Cursor: opts.Cursor,
		Limit:  int(opts.Limit),
	})
	if err != nil {
		return nil, err
	}

	return &ListResponse{
		Pairs:   res.Pairs,
		Cursor:  res.NextCursor,
		HasMore: res.HasMore,
	}, nil
}

// Scan performs a prefix scan with pagination support.
func (e *Etcd) Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}

	fullPrefix := e.prefix + opts.Prefix
	rangeEnd := clientv3.GetPrefixRangeEnd(fullPrefix)

	getOpts := []clientv3.OpOption{
		clientv3.WithRange(rangeEnd),
		clientv3.WithLimit(int64(opts.Limit + 1)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}

	key := fullPrefix
	if opts.Cursor != "" {
		// Start from the next key after the cursor
		key = e.prefix + opts.Cursor + "\x00"
	}

	resp, err := e.client.Get(ctx, key, getOpts...)
	if err != nil {
		return nil, fmt.Errorf("scan keys failed: %w", err)
	}

	res := &ScanResponse{
		Pairs: make([]Pair, 0, len(resp.Kvs)),
	}

	count := 0
	for _, kv := range resp.Kvs {
		if count >= opts.Limit {
			res.HasMore = true
			break
		}
		res.Pairs = append(res.Pairs, Pair{
			Key:   e.extractKey(string(kv.Key)),
			Value: string(kv.Value),
		})
		count++
	}

	if res.HasMore && len(res.Pairs) > 0 {
		res.NextCursor = res.Pairs[len(res.Pairs)-1].Key
	}

	return res, nil
}

// PutBatch stores multiple key-value pairs using a transaction.
func (e *Etcd) PutBatch(ctx context.Context, pairs []Pair, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	if ttl == TTLKeep {
		for _, p := range pairs {
			if err := e.Put(ctx, p.Key, p.Value, ttl); err != nil {
				return err
			}
		}
		return nil
	}

	var leaseID clientv3.LeaseID
	resp, err := e.client.Grant(ctx, ttlToSeconds(ttl))
	if err != nil {
		return err
	}
	leaseID = resp.ID

	ops := make([]clientv3.Op, 0, len(pairs))
	for _, p := range pairs {
		fk, err := e.buildKey(p.Key)
		if err != nil {
			_, _ = e.client.Revoke(context.Background(), leaseID)
			return err
		}
		ops = append(ops, clientv3.OpPut(fk, p.Value, clientv3.WithLease(leaseID)))
	}

	_, err = e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		_, _ = e.client.Revoke(context.Background(), leaseID)
	}
	return err
}

// GetBatch retrieves values for multiple keys using a transaction.
func (e *Etcd) GetBatch(ctx context.Context, keys []string) ([]string, error) {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, k := range keys {
		fk, err := e.buildKey(k)
		if err != nil {
			return nil, err
		}
		ops = append(ops, clientv3.OpGet(fk))
	}

	resp, err := e.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}

	res := make([]string, len(keys))
	for i, r := range resp.Responses {
		getResp := r.GetResponseRange()
		if getResp.Count == 0 {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, keys[i])
		}
		res[i] = string(getResp.Kvs[0].Value)
	}
	return res, nil
}

// DeleteBatch removes multiple keys using a transaction.
func (e *Etcd) DeleteBatch(ctx context.Context, keys []string) error {
	ops := make([]clientv3.Op, 0, len(keys))
	for _, k := range keys {
		fk, err := e.buildKey(k)
		if err != nil {
			return err
		}
		ops = append(ops, clientv3.OpDelete(fk))
	}
	_, err := e.client.Txn(ctx).Then(ops...).Commit()
	return err
}
