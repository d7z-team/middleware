package kv

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory 是一个内存KV存储实现
// Memory is an in-memory KV storage implementation.
// It can optionally persist data to disk, making it suitable for both testing and lightweight local storage.
type Memory struct {
	data   *sync.Map
	store  string
	prefix string // key prefix, default is empty
}

// Count 统计指定前缀下的有效键数量（排除过期键）
// Count counts the number of valid keys (excluding expired ones) with the specified prefix.
func (m *Memory) Count(ctx context.Context) (int64, error) {
	var count int64
	now := time.Now()

	m.data.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		k, ok := key.(string)
		if !ok {
			return true
		}
		if !strings.HasPrefix(k, m.prefix) {
			return true
		}

		// Check value type and expiration
		content, ok := value.(memoryContent)
		if !ok {
			return true
		}
		if content.TTL != nil && now.After(*content.TTL) {
			return true
		}

		count++
		return true
	})
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	return count, nil
}

// NewMemory 创建一个新的内存存储实例
// NewMemory creates a new memory storage instance.
// If store is provided, it tries to load data from the file and persist updates to it.
func NewMemory(store string) (*Memory, error) {
	ret := &Memory{
		store:  store,
		data:   &sync.Map{},
		prefix: "", // default empty
	}
	if store != "" {
		if err := os.MkdirAll(filepath.Dir(store), 0o755); err != nil && !os.IsExist(err) {
			return nil, err
		}
		item := make(map[string]memoryContent)
		data, err := os.ReadFile(store)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err == nil {
			err = json.Unmarshal(data, &item)
			if err != nil {
				return nil, err
			}
		}
		for key, content := range item {
			if content.TTL == nil || time.Now().Before(*content.TTL) {
				ret.data.Store(key, content)
			}
		}
		clear(item)
	}
	return ret, nil
}

// memoryContent 存储内存中的键值对及过期时间
// memoryContent stores the key-value pair and expiration time in memory.
type memoryContent struct {
	Data     string     `json:"data"`
	CreateAt time.Time  `json:"create_at"`
	TTL      *time.Time `json:"ttl,omitempty"`
}

type memoryKv struct {
	Key string
	Val memoryContent
}

// Child creates a child KV with appended path.
func (m *Memory) Child(paths ...string) KV {
	if len(paths) == 0 {
		return m
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
		return m
	}
	return &Memory{
		data:   m.data,
		store:  "",
		prefix: m.prefix + strings.Join(keys, "/") + "/",
	}
}

// Sync 将内存中的数据同步到文件
// Sync persists the in-memory data to the configured file.
func (m *Memory) Sync() error {
	if m.store == "" {
		return nil
	}

	// Collect non-expired data
	item := make(map[string]memoryContent)
	now := time.Now()
	m.data.Range(func(key, value interface{}) bool {
		content, ok := value.(memoryContent)
		if ok && (content.TTL == nil || now.Before(*content.TTL)) {
			// Only store keys with correct prefix
			k, ok := key.(string)
			if ok && strings.HasPrefix(k, m.prefix) {
				item[k] = content
			}
		}
		return true
	})

	saved, err := json.Marshal(item)
	if err != nil {
		return err
	}

	// Write to temporary file first, then atomically rename to avoid corruption
	tempFile := m.store + ".tmp"
	if err := os.WriteFile(tempFile, saved, 0o600); err != nil {
		return err
	}

	if err := os.Rename(tempFile, m.store); err != nil {
		_ = os.Remove(tempFile) // Cleanup temp file
		return err
	}
	return nil
}

// listSorted retrieves and sorts all valid (non-expired) Pairs matching the prefix.
func (m *Memory) listSorted(ctx context.Context, prefix string, sortByTime bool) ([]Pair, error) {
	internal, err := m.listInternal(ctx, prefix)
	if err != nil {
		return nil, err
	}
	temp := make([]memoryKv, 0, len(internal))
	for k, v := range internal {
		temp = append(temp, memoryKv{Key: k, Val: v})
	}
	if sortByTime {
		sort.Slice(temp, func(i, j int) bool {
			return temp[i].Val.CreateAt.Before(temp[j].Val.CreateAt)
		})
	} else {
		sort.Slice(temp, func(i, j int) bool {
			return temp[i].Key < temp[j].Key
		})
	}

	result := make([]Pair, 0, len(temp))
	for _, item := range temp {
		k := item.Key
		if m.prefix != "" {
			k = k[len(m.prefix):]
		}
		result = append(result, Pair{Key: k, Value: item.Val.Data})
	}
	return result, nil
}

// ListCursor implements cursor-based pagination.
func (m *Memory) ListCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	all, err := m.listSorted(ctx, "", false)
	if err != nil {
		return nil, err
	}
	start := listCursorStartIndex(all, opts.Cursor)
	limit := int(opts.Limit)
	if limit <= 0 {
		limit = len(all)
	}
	endIndex := start + limit
	if endIndex > len(all) {
		endIndex = len(all)
	}
	pairs := all[start:endIndex]
	hasMore := endIndex < len(all)
	var nextCursor string
	if hasMore {
		nextCursor = pairs[len(pairs)-1].Key
	}
	return &ListResponse{Pairs: pairs, Cursor: nextCursor, HasMore: hasMore}, nil
}

// ListCurrentCursor implements cursor-based pagination for current level.
func (m *Memory) ListCurrentCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	all, err := m.listSorted(ctx, "", false)
	if err != nil {
		return nil, err
	}
	filtered := make([]Pair, 0)
	for _, p := range all {
		if isCurrentLevel(p.Key, "") {
			filtered = append(filtered, p)
		}
	}
	start := listCursorStartIndex(filtered, opts.Cursor)
	limit := int(opts.Limit)
	if limit <= 0 {
		limit = len(filtered)
	}
	endIndex := start + limit
	if endIndex > len(filtered) {
		endIndex = len(filtered)
	}
	pairs := filtered[start:endIndex]
	hasMore := endIndex < len(filtered)
	var nextCursor string
	if hasMore {
		nextCursor = pairs[len(pairs)-1].Key
	}
	return &ListResponse{Pairs: pairs, Cursor: nextCursor, HasMore: hasMore}, nil
}

// ListPage retrieves paginated key-value pairs matching the prefix.
func (m *Memory) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := m.listSorted(ctx, prefix, true)
	if err != nil {
		return nil, err
	}
	start, end := listPageRange(len(all), pageIndex, pageSize)
	return all[start:end], nil
}

// ListCurrentPage returns a page of key-value pairs at the current level.
func (m *Memory) ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, err := m.listSorted(ctx, prefix, true)
	if err != nil {
		return nil, err
	}
	filtered := make([]Pair, 0)
	for _, p := range all {
		if isCurrentLevel(p.Key, prefix) {
			filtered = append(filtered, p)
		}
	}
	start, end := listPageRange(len(filtered), pageIndex, pageSize)
	return filtered[start:end], nil
}

// List retrieves all key-value pairs matching the prefix.
func (m *Memory) List(ctx context.Context, prefix string) ([]Pair, error) {
	return m.listSorted(ctx, prefix, false)
}

// ListCurrent returns key-value pairs at the current level.
func (m *Memory) ListCurrent(ctx context.Context, prefix string) ([]Pair, error) {
	all, err := m.listSorted(ctx, prefix, false)
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

func (m *Memory) listInternal(ctx context.Context, prefix string) (map[string]memoryContent, error) {
	result := make(map[string]memoryContent)
	now := time.Now()

	m.data.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		k, ok := key.(string)
		if !ok {
			return true // skip non-string keys
		}

		if !strings.HasPrefix(k, m.prefix+prefix) {
			return true
		}

		content, ok := value.(memoryContent)
		if !ok {
			return true // skip wrong value types
		}

		if content.TTL == nil || now.Before(*content.TTL) {
			result[k] = content
		}
		return true
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return result, nil
}

func (m *Memory) buildKey(key string) (string, error) {
	if strings.Contains(key, "/") {
		return "", ErrInvalidKey
	}
	return m.prefix + key, nil
}

// Put stores a key-value pair with an optional TTL.
func (m *Memory) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	fullKey, err := m.buildKey(key)
	if err != nil {
		return err
	}
	now := time.Now()

	var td *time.Time
	createAt := now

	if ttl == TTLKeep {
		if existing, ok := m.data.Load(fullKey); ok {
			if content, ok := existing.(memoryContent); ok {
				// Preserve existing TTL and CreationTime if still valid
				if content.TTL == nil || now.Before(*content.TTL) {
					td = content.TTL
					createAt = content.CreateAt
				}
			}
		}
	} else {
		d := now.Add(ttl)
		td = &d
	}

	m.data.Store(fullKey, memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: createAt,
	})
	return nil
}

// Get 获取指定键的值，过期则删除并返回不存在
// Get retrieves the value for a key. Returns ErrKeyNotFound if the key does not exist or has expired.
func (m *Memory) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	// Add prefix to key
	fullKey, err := m.buildKey(key)
	if err != nil {
		return "", err
	}
	if value, ok := m.data.Load(fullKey); ok {
		content, ok := value.(memoryContent)
		if !ok {
			m.data.Delete(fullKey) // Cleanup invalid data
			return "", ErrKeyNotFound
		}

		now := time.Now()
		if content.TTL != nil && now.After(*content.TTL) {
			m.data.Delete(fullKey) // Delete expired key
			return "", ErrKeyNotFound
		}
		return content.Data, nil
	}
	return "", ErrKeyNotFound
}

// Delete 删除指定的键
// Delete removes the specified key.
func (m *Memory) Delete(ctx context.Context, key string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	// Add prefix to key
	fullKey, err := m.buildKey(key)
	if err != nil {
		return false, err
	}
	_, loaded := m.data.LoadAndDelete(fullKey)
	return loaded, nil
}

// DeleteAll removes all keys under the current prefix.
func (m *Memory) DeleteAll(ctx context.Context) error {
	m.data.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		k, ok := key.(string)
		if !ok {
			return true
		}
		if strings.HasPrefix(k, m.prefix) {
			m.data.Delete(k)
		}
		return true
	})
	return ctx.Err()
}

// PutIfNotExists 仅在键不存在时设置值（原子操作）
// PutIfNotExists sets the value only if the key does not exist (atomic operation).
func (m *Memory) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	fullKey, err := m.buildKey(key)
	if err != nil {
		return false, err
	}
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	newValue := memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: now,
	}

	// Use LoadOrStore for atomicity: store if not exists, return true; return false if exists
	actual, loaded := m.data.LoadOrStore(fullKey, newValue)
	if loaded {
		// Check if the existing value is expired
		content, ok := actual.(memoryContent)
		if !ok {
			// Type error, replace with new value
			m.data.Store(fullKey, newValue)
			return true, nil
		}

		if content.TTL != nil && now.After(*content.TTL) {
			// Expired, replace with new value (use CAS for atomicity)
			swapped := m.data.CompareAndSwap(fullKey, content, newValue)
			return swapped, nil
		}
		// Key exists and is valid, return false
		return false, nil
	}
	// Key did not exist, stored successfully
	return true, nil
}

// CompareAndSwap 当当前值等于oldValue时，将其更新为newValue（原子操作）
// CompareAndSwap updates the value to newValue only if the current value matches oldValue (atomic operation).
func (m *Memory) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	// Add prefix to key
	fullKey, err := m.buildKey(key)
	if err != nil {
		return false, err
	}
	val, exists := m.data.Load(fullKey)
	if !exists {
		return false, nil
	}
	content, ok := val.(memoryContent)
	if !ok {
		return false, nil
	}

	// Check if expired
	now := time.Now()
	if content.TTL != nil && now.After(*content.TTL) {
		return false, nil
	}

	// Compare current value with old value
	if content.Data != oldValue {
		return false, nil
	}

	// Prepare new value (preserve original TTL and creation time)
	newContent := memoryContent{
		Data:     newValue,
		TTL:      content.TTL,
		CreateAt: content.CreateAt,
	}

	// Use CAS atomic operation to replace
	swapped := m.data.CompareAndSwap(fullKey, content, newContent)
	return swapped, nil
}
