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

// Memory 一个简单的内存配置存储，仅用于测试，实现了KV接口
type Memory struct {
	data   *sync.Map
	store  string
	prefix string // 添加 prefix 字段，默认为空字符串
}

// NewMemory 创建一个新的内存存储实例
func NewMemory(store string) (*Memory, error) {
	ret := &Memory{
		store:  store,
		data:   &sync.Map{},
		prefix: "", // 默认空字符串
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
type memoryContent struct {
	Data     string     `json:"data"`
	CreateAt time.Time  `json:"create_at"`
	TTL      *time.Time `json:"ttl,omitempty"`
}

type memoryKv struct {
	Key string
	Val memoryContent
}

func (m *Memory) Child(path string) KV {
	path = m.prefix + strings.Trim(path, "/") + "/"
	if path == "" {
		return m
	}
	return &Memory{
		data:   m.data,
		store:  "",
		prefix: path,
	}
}

// Sync 将内存中的数据同步到文件
func (m *Memory) Sync() error {
	if m.store == "" {
		return nil
	}

	// 收集未过期的数据
	item := make(map[string]memoryContent)
	now := time.Now()
	m.data.Range(func(key, value interface{}) bool {
		content, ok := value.(memoryContent)
		if ok && (content.TTL == nil || now.Before(*content.TTL)) {
			// 只存储带有正确 prefix 的键
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

	// 先写入临时文件，成功后原子性重命名，避免文件损坏
	tempFile := m.store + ".tmp"
	if err := os.WriteFile(tempFile, saved, 0o600); err != nil {
		return err
	}

	if err := os.Rename(tempFile, m.store); err != nil {
		_ = os.Remove(tempFile) // 清理临时文件
		return err
	}
	return nil
}

// CursorList 实现游标分页查询
func (m *Memory) CursorList(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	// 获取所有键并按字典序排序
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
			return true // 跳过非字符串键
		}

		// 添加 prefix 过滤
		if !strings.HasPrefix(k, m.prefix) {
			return true
		}

		content, ok := value.(memoryContent)
		if !ok {
			return true // 跳过类型错误的value
		}

		// 检查是否过期
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

	// 按键排序
	sort.Strings(allKeys)

	// 处理游标
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

	// 计算分页
	limit := opts.Limit
	if limit <= 0 {
		limit = int64(len(allKeys)) // 默认返回所有
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
	// 构建响应
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
			return true // 跳过非字符串键
		}

		if !strings.HasPrefix(k, m.prefix+prefix) {
			return true
		}

		content, ok := value.(memoryContent)
		if !ok {
			return true // 跳过类型错误的value
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
func (m *Memory) Put(_ context.Context, key, value string, ttl time.Duration) error {
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	// 添加 prefix 到 key
	fullKey := m.prefix + key
	m.data.Store(fullKey, memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: now,
	})
	return nil
}

// Get 获取指定键的值，过期则删除并返回不存在
func (m *Memory) Get(_ context.Context, key string) (string, error) {
	// 添加 prefix 到 key
	fullKey := m.prefix + key
	if value, ok := m.data.Load(fullKey); ok {
		content, ok := value.(memoryContent)
		if !ok {
			m.data.Delete(fullKey) // 清理无效数据
			return "", ErrKeyNotFound
		}

		now := time.Now()
		if content.TTL != nil && now.After(*content.TTL) {
			m.data.Delete(fullKey) // 删除过期键
			return "", ErrKeyNotFound
		}
		return content.Data, nil
	}
	return "", ErrKeyNotFound
}

// Delete 删除指定的键
func (m *Memory) Delete(_ context.Context, key string) (bool, error) {
	// 添加 prefix 到 key
	fullKey := m.prefix + key
	_, loaded := m.data.LoadAndDelete(fullKey)
	return loaded, nil
}

// PutIfNotExists 仅在键不存在时设置值（原子操作）
func (m *Memory) PutIfNotExists(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	// 添加 prefix 到 key
	fullKey := m.prefix + key
	newValue := memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: now,
	}

	// 使用LoadOrStore确保原子性：不存在则存储，返回true；存在则返回false
	actual, loaded := m.data.LoadOrStore(fullKey, newValue)
	if loaded {
		// 检查已存在的值是否过期
		content, ok := actual.(memoryContent)
		if !ok {
			// 类型错误，替换为新值
			m.data.Store(fullKey, newValue)
			return true, nil
		}

		if content.TTL != nil && now.After(*content.TTL) {
			// 已过期，替换为新值（使用CAS确保原子性）
			swapped := m.data.CompareAndSwap(fullKey, content, newValue)
			return swapped, nil
		}
		// 键有效存在，返回false
		return false, nil
	}
	// 键不存在，已成功存储
	return true, nil
}

// CompareAndSwap 当当前值等于oldValue时，将其更新为newValue（原子操作）
func (m *Memory) CompareAndSwap(_ context.Context, key, oldValue, newValue string) (bool, error) {
	// 添加 prefix 到 key
	fullKey := m.prefix + key
	val, exists := m.data.Load(fullKey)
	if !exists {
		return false, ErrKeyNotFound
	}
	content, ok := val.(memoryContent)
	if !ok {
		return false, nil
	}

	// 检查是否过期
	now := time.Now()
	if content.TTL != nil && now.After(*content.TTL) {
		return false, ErrKeyNotFound
	}

	// 比较当前值与旧值
	if content.Data != oldValue {
		return false, nil
	}

	// 准备新值（保留原有TTL和创建时间）
	newContent := memoryContent{
		Data:     newValue,
		TTL:      content.TTL,
		CreateAt: content.CreateAt,
	}

	// 使用CAS原子操作替换
	swapped := m.data.CompareAndSwap(fullKey, content, newContent)
	return swapped, nil
}
