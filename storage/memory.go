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
	if path == "" {
		return ""
	}
	return strings.TrimPrefix(strings.ReplaceAll(filepath.Clean(path), "\\", "/"), "./")
}

// Exists 检查指定路径的文件是否存在
func (m *Memory) Exists(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		return false
	}

	normalized := normalizePath(name)
	_, exists := m.files[normalized]
	return exists
}

// Pull 读取指定路径的文件内容，返回io.ReadCloser
func (m *Memory) Pull(path string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if path == "" {
		return nil, errors.New("path cannot be empty")
	}

	normalized := normalizePath(path)
	data, exists := m.files[normalized]
	if !exists {
		return nil, errors.New("file not found: " + path)
	}

	// 使用NopCloser包装字节读取器，实现ReadCloser接口
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Push 写入内容到指定路径
func (m *Memory) Push(path string, r io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if path == "" {
		return errors.New("path cannot be empty")
	}

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

	// 处理根目录情况
	var prefix string
	if normalizedPath == "" {
		prefix = ""
	} else {
		prefix = normalizedPath + "/"
	}

	seen := make(map[string]struct{}) // 用于去重
	var entries []string

	for file := range m.files {
		// 检查是否为当前路径的子条目
		if prefix != "" && !strings.HasPrefix(file, prefix) {
			continue
		}
		if prefix == "" && strings.Contains(file, "/") {
			// 对于根目录，只显示第一级目录
			firstPart := strings.SplitN(file, "/", 2)[0]
			if _, ok := seen[firstPart]; !ok {
				seen[firstPart] = struct{}{}
				entries = append(entries, firstPart)
			}
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

// Remove 删除指定路径的文件或目录（包括所有子文件）
func (m *Memory) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if path == "" {
		return errors.New("path cannot be empty")
	}

	normalized := normalizePath(path)

	// 检查是否是文件路径
	if _, exists := m.files[normalized]; exists {
		// 如果是文件，直接删除
		delete(m.files, normalized)
		return nil
	}

	// 如果是目录路径，删除所有以该路径开头的文件
	prefix := normalized + "/"
	var found bool
	for file := range m.files {
		if strings.HasPrefix(file, prefix) {
			delete(m.files, file)
			found = true
		}
	}

	if !found {
		return errors.New("path not found: " + path)
	}

	return nil
}

// Close 关闭存储并清理资源
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清空所有文件
	m.files = make(map[string][]byte)
	return nil
}
