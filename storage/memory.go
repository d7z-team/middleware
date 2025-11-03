package storage

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
)

// Memory 实现了Storage接口的内存存储版本
type Memory struct {
	files map[string][]byte // 存储文件路径到内容的映射
	mu    sync.Mutex        // 保证并发安全
}

// NewMemory 创建一个新的内存存储实例
func NewMemory() Storage {
	return &Memory{
		files: make(map[string][]byte),
	}
}

// normalizePath 规范化路径，统一使用"/"作为分隔符，移除冗余斜杠
func normalizePath(path string) string {
	// 清理路径（处理.和..）
	cleaned := filepath.Clean(path)
	// 统一替换为正斜杠（跨平台兼容）
	return strings.ReplaceAll(cleaned, "\\", "/")
}

// Exists 检查指定路径的文件是否存在
func (m *Memory) Exists(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalized := normalizePath(name)
	_, exists := m.files[normalized]
	return exists
}

// Pull 读取指定路径的文件内容，返回io.ReadCloser
func (m *Memory) Pull(path string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalized := normalizePath(path)
	data, exists := m.files[normalized]
	if !exists {
		return nil, errors.New("file not found")
	}

	// 使用NopCloser包装字节读取器，实现ReadCloser接口
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Push 写入内容到指定路径
func (m *Memory) Push(path string, r io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalized := normalizePath(path)
	// 读取所有内容
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.files[normalized] = data
	return nil
}

// List 列出指定路径下的所有条目（文件和子目录）
func (m *Memory) List(path string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedPath := normalizePath(path)
	prefix := normalizedPath
	// 处理根目录情况（path为空时前缀为空）
	if prefix != "" {
		prefix += "/"
	}

	seen := make(map[string]struct{}) // 用于去重
	var entries []string

	for file := range m.files {
		// 检查是否为当前路径的子条目
		if !strings.HasPrefix(file, prefix) {
			continue
		}

		// 提取相对路径
		relative := strings.TrimPrefix(file, prefix)
		if relative == "" {
			continue // 跳过自身路径
		}

		// 分割路径获取第一个层级的条目名
		parts := strings.SplitN(relative, "/", 2)
		entry := parts[0]

		if _, ok := seen[entry]; !ok {
			seen[entry] = struct{}{}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// Remove 删除指定路径的文件
func (m *Memory) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalized := normalizePath(path)
	if _, exists := m.files[normalized]; !exists {
		return errors.New("file not found")
	}

	delete(m.files, normalized)
	return nil
}

func (m *Memory) Close() error {
	return nil
}
