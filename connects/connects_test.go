package connects

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEtcd(t *testing.T) {
	parse, _ := url.Parse("etcd://127.0.0.1:2379")
	etcd, err := NewEtcd(parse)
	if err != nil {
		t.Skip("如需测试 etcd 实现，请确保本地 etcd 运行")
	}
	defer etcd.Close()
}

func TestNewS3(t *testing.T) {
	parse, _ := url.Parse("s3://minio_admin:minio_admin@127.0.0.1:9000/test-bucket")
	s3, err := NewS3(parse)
	if err != nil {
		t.Skip("如需测试 s3 实现，请确保本地 s3 运行")
	}
	assert.Equal(t, "test-bucket", s3.BucketName)
}

func TestNewRedis(t *testing.T) {
	parse, _ := url.Parse("redis://127.0.0.1:6379")
	redis, err := NewRedis(parse)
	if err != nil {
		t.Skip("如需测试 s3 实现，请确保本地 s3 运行")
	}
	defer redis.Close()
	ping := redis.Ping(t.Context())
	assert.NoError(t, ping.Err())
}
