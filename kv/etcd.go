package kv

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	key = strings.TrimPrefix(key, "/")
	return e.prefix + key, nil
}

func (e *Etcd) extractKey(fullKey string) string {
	return strings.TrimPrefix(fullKey, e.prefix)
}

// List returns all key-value pairs matching the prefix.
func (e *Etcd) List(ctx context.Context, prefix string) (map[string]string, error) {
	fullPrefix := e.prefix + prefix
	resp, err := e.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list keys failed: %w", err)
	}

	result := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		relKey := e.extractKey(key)
		result[relKey] = string(kv.Value)
	}
	return result, nil
}

// ListCurrent returns key-value pairs at the current level (excluding children).
func (e *Etcd) ListCurrent(ctx context.Context, prefix string) (map[string]string, error) {
	fullPrefix := e.prefix + prefix
	resp, err := e.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list current keys failed: %w", err)
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		relKey := e.extractKey(key)

		// Logic similar to Memory
		if !strings.HasPrefix(relKey, prefix) {
			continue
		}

		check := relKey[len(prefix):]
		check = strings.TrimPrefix(check, "/")
		if strings.Contains(check, "/") {
			continue
		}
		if check == "" {
			continue
		}

		result[relKey] = string(kv.Value)
	}
	return result, nil
}

// ListPage returns a page of key-value pairs matching the prefix.
func (e *Etcd) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	// For Etcd, efficient pagination requires using the key from the previous page as a starting point (cursor).
	// Since ListPage uses numeric index, we have to fetch all keys or use a very inefficient skip.
	// For consistency with other implementations and the interface, we fetch all (keys only first?) then slice.
	// But ListPage returns values too.
	// Optimization: Fetch only keys first, determine the range, then fetch values.
	// Or just fetch everything if dataset is small.
	// Given typical use of ListPage implies small datasets or bad design, let's do the safe implementation:
	// Fetch all matching keys, sort, slice.

	fullPrefix := e.prefix + prefix
	// Get all keys with prefix
	resp, err := e.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list page failed: %w", err)
	}

	// Collect and sort
	type kvPair struct {
		key string
		val string
	}
	kvs := make([]kvPair, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		kvs = append(kvs, kvPair{key: string(item.Key), val: string(item.Value)})
	}

	// Sort by key (lexicographically)
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})

	start := uint64(pageSize) * pageIndex
	end := start + uint64(pageSize)

	result := make(map[string]string)
	if start >= uint64(len(kvs)) {
		return result, nil
	}
	if end > uint64(len(kvs)) {
		end = uint64(len(kvs))
	}

	for _, item := range kvs[start:end] {
		relKey := e.extractKey(item.key)
		result[relKey] = item.val
	}

	return result, nil
}

// ListCurrentPage returns a page of key-value pairs at the current level (excluding children).
func (e *Etcd) ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	// Fetch all to filter (inefficient but safe for "current level" semantics)
	fullPrefix := e.prefix + prefix
	resp, err := e.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list current page failed: %w", err)
	}

	type kvPair struct {
		key string
		val string
	}
	kvs := make([]kvPair, 0)

	for _, item := range resp.Kvs {
		key := string(item.Key)
		relKey := e.extractKey(key)

		// Filter
		if !strings.HasPrefix(relKey, prefix) {
			continue
		}

		check := relKey[len(prefix):]
		check = strings.TrimPrefix(check, "/")
		if strings.Contains(check, "/") {
			continue
		}
		if check == "" {
			continue
		}

		kvs = append(kvs, kvPair{key: relKey, val: string(item.Value)})
	}

	// Sort by Key (consistent with Etcd ListPage)
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].key < kvs[j].key
	})

	start := uint64(pageSize) * pageIndex
	end := start + uint64(pageSize)

	result := make(map[string]string)
	if start >= uint64(len(kvs)) {
		return result, nil
	}
	if end > uint64(len(kvs)) {
		end = uint64(len(kvs))
	}

	for _, item := range kvs[start:end] {
		result[item.key] = item.val
	}

	return result, nil
}

// CursorListCurrent implements cursor-based pagination for current level.
func (e *Etcd) CursorListCurrent(ctx context.Context, options *ListOptions) (*ListResponse, error) {
	opts := &ListOptions{}
	if options != nil {
		opts = options
	}
	if opts.Limit == 0 {
		opts.Limit = 1000
	}

	// Scan/Get all to filter correctly.
	// Optimizing with WithFromKey is risky if many children exist between cursor and next sibling.
	// We'll use the safe "Get All" approach for consistency with ListCurrent logic.

	resp, err := e.client.Get(ctx, e.prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, fmt.Errorf("cursor list current failed: %w", err)
	}

	filteredKeys := make([]string, 0)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		relKey := e.extractKey(key)

		if strings.Contains(relKey, "/") {
			continue
		}
		if relKey == "" {
			continue
		}
		filteredKeys = append(filteredKeys, relKey)
	}

	// Apply Cursor
	startIndex := 0
	if opts.Cursor != "" {
		for i, key := range filteredKeys {
			if key > opts.Cursor {
				startIndex = i
				break
			}
		}
		// If cursor > all or not found logic
		if startIndex == 0 && (len(filteredKeys) == 0 || filteredKeys[0] <= opts.Cursor) {
			found := false
			for i, key := range filteredKeys {
				if key > opts.Cursor {
					startIndex = i
					found = true
					break
				}
			}
			if !found {
				startIndex = len(filteredKeys)
			}
		}
	}

	// Apply Limit
	endIndex := startIndex + int(opts.Limit)
	if endIndex > len(filteredKeys) {
		endIndex = len(filteredKeys)
	}

	resultKeys := filteredKeys[startIndex:endIndex]

	var nextCursor string
	hasMore := endIndex < len(filteredKeys)
	if hasMore {
		nextCursor = resultKeys[len(resultKeys)-1]
	}

	return &ListResponse{
		Keys:    resultKeys,
		Cursor:  nextCursor,
		HasMore: hasMore,
	}, nil
}

// Put stores a key-value pair. If ttl is not TTLKeep, it sets a lease.
func (e *Etcd) Put(ctx context.Context, key, value string, ttl time.Duration) error {
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

	var opts []clientv3.OpOption
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
		key += opts.Cursor + "\x00"
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
			break
		}
		result.Keys = append(result.Keys, e.extractKey(key))
		count++
	}

	if result.HasMore && len(result.Keys) > 0 {
		result.Cursor = result.Keys[len(result.Keys)-1]
	}

	return result, nil
}
