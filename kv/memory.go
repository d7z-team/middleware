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

// CursorList 实现游标分页查询
// CursorList implements cursor-based pagination.
func (m *Memory) CursorList(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	// Get all keys and sort them lexicographically
	allKeys := make([]string, 0)
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

		// Filter by prefix
		if !strings.HasPrefix(k, m.prefix) {
			return true
		}

		content, ok := value.(memoryContent)
		if !ok {
			return true // skip wrong value types
		}

		// Check expiration
		if content.TTL != nil && now.After(*content.TTL) {
			return true
		}
		allKeys = append(allKeys, k)
		return true
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Sort keys
	sort.Strings(allKeys)

	// Handle cursor
	startIndex := 0
	if opts.Cursor != "" {
		if m.prefix != "" {
			opts.Cursor = m.prefix + opts.Cursor
		}
		for i, key := range allKeys {
			if key > opts.Cursor {
				startIndex = i
				break
			}
		}
	}

	// Calculate pagination
	limit := opts.Limit
	if limit <= 0 {
		limit = int64(len(allKeys)) // Default return all
	}

	endIndex := startIndex + int(limit)
	if endIndex > len(allKeys) {
		endIndex = len(allKeys)
	}
	if m.prefix != "" {
		for i := range allKeys {
			allKeys[i] = allKeys[i][len(m.prefix):]
		}
	}
	// Build response
	keys := allKeys[startIndex:endIndex]
	hasMore := endIndex < len(allKeys)
	var nextCursor string
	if hasMore {
		nextCursor = keys[len(keys)-1]
	}

	return &ListResponse{
		Keys:    keys,
		Cursor:  nextCursor,
		HasMore: hasMore,
	}, nil
}

// ListPage 分页获取前缀匹配的键值对
// ListPage retrieves paginated key-value pairs matching the prefix.
func (m *Memory) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	internal, err := m.listInternal(ctx, prefix)
	if err != nil {
		return nil, err
	}

	temp := make([]memoryKv, 0, len(internal))
	for k, v := range internal {
		temp = append(temp, memoryKv{Key: k, Val: v})
	}

	sort.Slice(temp, func(i, j int) bool {
		return temp[i].Val.CreateAt.Before(temp[j].Val.CreateAt)
	})

	result := make(map[string]string)
	start := int(pageSize * uint(pageIndex))
	end := int(pageSize * uint(pageIndex+1))

	if start >= len(temp) {
		return result, nil
	}

	if end > len(temp) {
		end = len(temp)
	}

	for _, item := range temp[start:end] {
		if m.prefix != "" {
			item.Key = item.Key[len(m.prefix):]
		}
		result[item.Key] = item.Val.Data
	}
	return result, nil
}

// List retrieves all key-value pairs matching the prefix.
func (m *Memory) List(ctx context.Context, prefix string) (map[string]string, error) {
	result := make(map[string]string)
	internal, err := m.listInternal(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for k, v := range internal {
		if m.prefix != "" {
			k = k[len(m.prefix):]
		}
		result[k] = v.Data
	}
	return result, nil
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

// Put 存储键值对，可以设置过期时间
// Put stores a key-value pair with an optional TTL.
func (m *Memory) Put(_ context.Context, key, value string, ttl time.Duration) error {
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	// Add prefix to key
	fullKey := m.prefix + key
	m.data.Store(fullKey, memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: now,
	})
	return nil
}

// Get 获取指定键的值，过期则删除并返回不存在
// Get retrieves the value for a key. Returns ErrKeyNotFound if the key does not exist or has expired.
func (m *Memory) Get(_ context.Context, key string) (string, error) {
	// Add prefix to key
	fullKey := m.prefix + key
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
func (m *Memory) Delete(_ context.Context, key string) (bool, error) {
	// Add prefix to key
	fullKey := m.prefix + key
	_, loaded := m.data.LoadAndDelete(fullKey)
	return loaded, nil
}

// PutIfNotExists 仅在键不存在时设置值（原子操作）
// PutIfNotExists sets the value only if the key does not exist (atomic operation).
func (m *Memory) PutIfNotExists(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	// Add prefix to key
	fullKey := m.prefix + key
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
func (m *Memory) CompareAndSwap(_ context.Context, key, oldValue, newValue string) (bool, error) {
	// Add prefix to key
	fullKey := m.prefix + key
	val, exists := m.data.Load(fullKey)
	if !exists {
		return false, ErrKeyNotFound
	}
	content, ok := val.(memoryContent)
	if !ok {
		return false, nil
	}

	// Check if expired
	now := time.Now()
	if content.TTL != nil && now.After(*content.TTL) {
		return false, ErrKeyNotFound
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
