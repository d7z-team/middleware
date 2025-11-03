package storage

import (
	"io"
	"os"
	"path/filepath"
)

// Local 本地文件系统存储实现
type Local struct {
	baseDir string // 基础目录，所有操作都基于此目录
}

// NewLocal 创建一个新的本地存储实例
func NewLocal(baseDir string) (*Local, error) {
	// 确保基础目录存在
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	return &Local{baseDir: baseDir}, nil
}

// getFullPath 拼接基础目录和相对路径，获取完整路径
func (l *Local) getFullPath(path string) string {
	return filepath.Join(l.baseDir, path)
}

func (l *Local) Exists(path string) bool {
	stat, err := os.Stat(l.getFullPath(path))
	return err == nil && !stat.IsDir()
}

// Pull 从指定路径读取数据
func (l *Local) Pull(path string) (io.ReadCloser, error) {
	fullPath := l.getFullPath(path)
	return os.Open(fullPath)
}

// Push 将数据写入指定路径
func (l *Local) Push(path string, r io.Reader) error {
	fullPath := l.getFullPath(path)

	// 创建父目录
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	// 创建文件并写入内容
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	// 将数据从reader复制到文件
	_, err = io.Copy(file, r)
	return err
}

// List 列出指定路径下的所有项目
func (l *Local) List(path string) ([]string, error) {
	fullPath := l.getFullPath(path)

	// 检查路径是否存在
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	// 如果是文件，直接返回该文件
	if !info.IsDir() {
		return []string{path}, nil
	}

	// 读取目录内容
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	// 收集所有条目名称
	var result []string
	for _, entry := range entries {
		result = append(result, filepath.Join(path, entry.Name()))
	}

	return result, nil
}

// Remove 删除指定路径的内容
func (l *Local) Remove(path string) error {
	fullPath := l.getFullPath(path)
	return os.RemoveAll(fullPath)
}

func (l *Local) Close() error {
	return nil
}
