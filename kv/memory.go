package kv

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Memory 一个简单的内存配置存储，仅用于测试，实现了KV接口
type Memory struct {
	data   sync.Map
	store  string
	closed *atomic.Bool
}

// NewMemory 创建一个新的内存存储实例
func NewMemory(store string) (*Memory, error) {
	ret := &Memory{
		store:  store,
		data:   sync.Map{},
		closed: new(atomic.Bool),
	}
	if store != "" {
		zap.L().Info("parse config from store", zap.String("store", store))
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

func (m *Memory) WithKey(keys ...string) string {
	return strings.Join(keys, m.Splitter())
}

// ListPage 分页获取前缀匹配的键值对
func (m *Memory) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) (map[string]string, error) {
	if m.closed.Load() {
		return nil, ErrClosed
	}
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
		result[item.Key] = item.Val.Data
	}
	return result, nil
}

func (m *Memory) List(ctx context.Context, prefix string) (map[string]string, error) {
	if m.closed.Load() {
		return nil, ErrClosed
	}
	result := make(map[string]string)
	internal, err := m.listInternal(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for k, v := range internal {
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
			zap.L().Warn("invalid key type in Memory", zap.Any("key", key))
			return true // 跳过非字符串键
		}

		if !strings.HasPrefix(k, prefix) {
			return true
		}

		content, ok := value.(memoryContent)
		if !ok {
			zap.L().Warn("invalid value type in Memory", zap.String("key", k), zap.Any("value", value))
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
	if m.closed.Load() {
		return ErrClosed
	}
	now := time.Now()
	var td *time.Time
	if ttl != -1 {
		d := now.Add(ttl)
		td = &d
	}

	m.data.Store(key, memoryContent{
		Data:     value,
		TTL:      td,
		CreateAt: now,
	})
	return nil
}

// Get 获取指定键的值，过期则删除并返回不存在
func (m *Memory) Get(_ context.Context, key string) (string, error) {
	if m.closed.Load() {
		return "", ErrClosed
	}
	if value, ok := m.data.Load(key); ok {
		content, ok := value.(memoryContent)
		if !ok {
			m.data.Delete(key) // 清理无效数据
			return "", ErrKeyNotFound
		}

		now := time.Now()
		if content.TTL != nil && now.After(*content.TTL) {
			m.data.Delete(key) // 删除过期键
			return "", ErrKeyNotFound
		}
		return content.Data, nil
	}
	return "", ErrKeyNotFound
}

// Delete 删除指定的键
func (m *Memory) Delete(_ context.Context, key string) (bool, error) {
	if m.closed.Load() {
		return false, ErrClosed
	}
	_, loaded := m.data.LoadAndDelete(key)
	return loaded, nil
}

// PutIfNotExists 仅在键不存在时设置值（原子操作）
func (m *Memory) PutIfNotExists(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	if m.closed.Load() {
		return false, ErrClosed
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

	// 使用LoadOrStore确保原子性：不存在则存储，返回true；存在则返回false
	actual, loaded := m.data.LoadOrStore(key, newValue)
	if loaded {
		// 检查已存在的值是否过期
		content, ok := actual.(memoryContent)
		if !ok {
			// 类型错误，替换为新值
			m.data.Store(key, newValue)
			return true, nil
		}

		if content.TTL != nil && now.After(*content.TTL) {
			// 已过期，替换为新值（使用CAS确保原子性）
			swapped := m.data.CompareAndSwap(key, content, newValue)
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
	if m.closed.Load() {
		return false, ErrClosed
	}
	val, exists := m.data.Load(key)
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
	swapped := m.data.CompareAndSwap(key, content, newContent)
	return swapped, nil
}

// Close 关闭内存存储，安全持久化数据到文件
func (m *Memory) Close() error {
	if m.closed.Swap(true) {
		return ErrClosed
	}
	defer m.data.Clear()
	if m.store == "" {
		return nil
	}

	// 收集未过期的数据
	item := make(map[string]memoryContent)
	now := time.Now()
	m.data.Range(func(key, value interface{}) bool {
		content, ok := value.(memoryContent)
		if ok && (content.TTL == nil || now.Before(*content.TTL)) {
			item[key.(string)] = content
		}
		return true
	})

	zap.L().Debug("回写内容到本地存储", zap.String("store", m.store), zap.Int("length", len(item)))
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

func (m *Memory) Splitter() string {
	return "::"
}
