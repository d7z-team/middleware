package storage

import (
	"context"
	"io"
	"path/filepath"

	"github.com/minio/minio-go/v7"
)

// S3 S3兼容存储(如MinIO)的实现
type S3 struct {
	client     *minio.Client
	bucketName string
	ctx        context.Context
}

// NewS3 创建一个新的S3存储实例
func NewS3(client *minio.Client, bucket string) (*S3, error) {
	// 检查桶是否存在，如果不存在则创建
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}

	return &S3{
		client:     client,
		bucketName: bucket,
		ctx:        ctx,
	}, nil
}

// Pull 从S3存储中获取指定路径的对象
func (s *S3) Pull(path string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(s.ctx, s.bucketName, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// Push 将数据写入S3存储的指定路径
func (s *S3) Push(path string, r io.Reader) error {
	// 获取对象大小
	size := getReaderSize(r)

	// 上传对象
	_, err := s.client.PutObject(
		s.ctx,
		s.bucketName,
		path,
		r,
		size,
		minio.PutObjectOptions{},
	)
	return err
}

// List 列出S3存储中指定路径下的所有对象
func (s *S3) List(path string) ([]string, error) {
	// 确保路径以斜杠结尾，以便正确列出子目录
	prefix := path
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}

	var objects []string
	// 列出所有对象
	for object := range s.client.ListObjects(
		s.ctx,
		s.bucketName,
		minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: false,
		},
	) {
		if object.Err != nil {
			return nil, object.Err
		}
		// 去除前缀，只保留相对路径
		name := object.Key
		if prefix != "" {
			name = name[len(prefix):]
		}
		if name != "" {
			objects = append(objects, filepath.Join(path, name))
		}
	}

	return objects, nil
}

// Remove 从S3存储中删除指定路径的对象
func (s *S3) Remove(path string) error {
	// 检查是否是目录
	objects, err := s.List(path)
	if err != nil {
		return err
	}

	// 如果是目录，删除所有子对象
	if len(objects) > 0 {
		for _, objPath := range objects {
			if err := s.client.RemoveObject(s.ctx, s.bucketName, objPath, minio.RemoveObjectOptions{}); err != nil {
				return err
			}
		}
	}

	// 删除指定路径的对象
	return s.client.RemoveObject(s.ctx, s.bucketName, path, minio.RemoveObjectOptions{})
}

// Exists 检查S3存储中指定路径的对象是否存在
func (s *S3) Exists(path string) bool {
	// 尝试获取对象信息
	_, err := s.client.StatObject(s.ctx, s.bucketName, path, minio.StatObjectOptions{})
	if err != nil {
		return false
	}
	// 没有错误，说明对象存在
	return true
}

func (s *S3) Close() error {
	return nil
}

// getReaderSize 尝试获取reader的大小
func getReaderSize(r io.Reader) int64 {
	if lr, ok := r.(*io.LimitedReader); ok {
		return lr.N
	}
	if sr, ok := r.(interface {
		Size() int64
	}); ok {
		return sr.Size()
	}
	// 如果无法获取大小，返回-1，MinIO会自动处理
	return -1
}
