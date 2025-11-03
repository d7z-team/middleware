package storage

import (
	"errors"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
	if u == "" ||
		strings.HasPrefix(u, "./") ||
		strings.HasPrefix(u, "/") ||
		strings.HasPrefix(u, "\\") ||
		strings.HasPrefix(u, ".\\") {
		return newLocalStorageFromURL(u)
	}
	// 解析URL
	ur, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	// 根据不同的协议方案创建对应的存储实例
	switch ur.Scheme {
	case "file":
		// 本地文件存储
		return newLocalStorageFromURL(ur.Path)
	case "s3", "minio":
		// S3或MinIO存储
		return newS3StorageFromURL(ur)
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

// newS3StorageFromURL 从URL创建S3/MinIO存储实例
func newS3StorageFromURL(ur *url.URL) (Storage, error) {
	// 解析访问密钥和密钥
	var accessKey, secretKey string
	if ur.User != nil {
		accessKey = ur.User.Username()
		secretKey, _ = ur.User.Password()
	}

	if accessKey == "" || secretKey == "" {
		return nil, errors.New("s3 URL必须包含访问密钥和密钥")
	}

	// 解析端点(包含主机和端口)
	endpoint := ur.Host
	if endpoint == "" {
		return nil, errors.New("s3 URL必须包含端点")
	}

	// 解析存储桶名称(URL路径的第一部分)
	bucketName := strings.TrimPrefix(ur.Path, "/")
	if i := strings.Index(bucketName, "/"); i != -1 {
		bucketName = bucketName[:i] // 只取路径的第一部分作为桶名
	}

	if bucketName == "" {
		return nil, errors.New("s3 URL必须包含存储桶名称")
	}

	// 解析查询参数
	secure := false
	if sslStr := ur.Query().Get("secure"); sslStr == "false" {
		secure = true
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	return NewS3(client, bucketName)
}
