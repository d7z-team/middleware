package storage

import (
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"gopkg.d7z.net/middleware/connects"
)

type Storage interface {
	Exists(name string) bool
	Pull(path string) (io.ReadCloser, error)
	Push(path string, r io.Reader) error
	List(path string) ([]string, error)
	Remove(path string) error
	Close() error
}

// NewStorageFromURL 根据URL创建相应的存储实例
// URL格式示例:
// - 本地存储: file:///path/to/storage
// - S3/MinIO: s3://accessKey:secretKey@endpoint:port/bucket?secure=true
func NewStorageFromURL(u string) (Storage, error) {
	// 解析URL
	ur, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	// 根据不同的协议方案创建对应的存储实例
	switch ur.Scheme {
	case "file", "local":
		// 本地文件存储
		return newLocalStorageFromURL(ur.Path)
	case "s3", "minio":
		// S3或MinIO存储
		s3, err := connects.NewS3(ur)
		if err != nil {
			return nil, err
		}
		return NewS3(s3.Client, s3.BucketName)
	case "memory", "mem":
		return NewMemory(), nil
	default:
		return nil, errors.New("不支持的存储协议: " + ur.Scheme)
	}
}

// newLocalStorageFromURL 从URL创建本地存储实例
func newLocalStorageFromURL(path string) (Storage, error) {
	if strings.HasPrefix(path, "/") && len(path) > 1 && path[1] == ':' {
		path = path[1:] // 移除开头的斜杠，如将"/C:/data"转换为"C:/data"
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	return NewLocal(absPath)
}
