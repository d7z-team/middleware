package connects

import (
	"errors"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Client struct {
	*minio.Client
	BucketName string
}

func NewS3(url *url.URL) (*S3Client, error) {
	// 解析访问密钥和密钥
	var accessKey, secretKey string
	if url.User != nil {
		accessKey = url.User.Username()
		secretKey, _ = url.User.Password()
	}

	if accessKey == "" || secretKey == "" {
		return nil, errors.New("s3 URL必须包含访问密钥和密钥")
	}

	// 解析端点(包含主机和端口)
	endpoint := url.Host
	if endpoint == "" {
		return nil, errors.New("s3 URL必须包含端点")
	}

	// 解析存储桶名称(URL路径的第一部分)
	bucketName := strings.TrimPrefix(url.Path, "/")
	if i := strings.Index(bucketName, "/"); i != -1 {
		bucketName = bucketName[:i] // 只取路径的第一部分作为桶名
	}

	if bucketName == "" {
		return nil, errors.New("s3 URL必须包含存储桶名称")
	}

	// 解析查询参数
	secure := false
	if sslStr := url.Query().Get("secure"); sslStr == "false" {
		secure = true
	}
	region := url.Query().Get("region")
	if region == "" {
		region = "us-east-1"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: region,
	})
	if err != nil {
		return nil, err
	}
	return &S3Client{
		Client:     client,
		BucketName: bucketName,
	}, nil
}
