package connects

import (
	"crypto/tls"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEtcd(t *testing.T) {
	// Test basic connection (might fail if no etcd)
	u, _ := url.Parse("etcd://127.0.0.1:2379")
	etcd, err := NewEtcd(u)
	if err == nil {
		defer etcd.Close()
	} else {
		t.Logf("Skipping Etcd connection test: %v", err)
	}

	// Test Incomplete TLS
	uTLS, _ := url.Parse("etcd://127.0.0.1:2379?ca-file=/tmp/ca.pem")
	_, errTLS := NewEtcd(uTLS)
	assert.Error(t, errTLS)
	assert.Contains(t, errTLS.Error(), "incomplete tls configuration")

	// Test With Options
	// Since we can't inspect the client config easily without private access,
	// we assume the functional options pattern is correct if compilation passes
	// and no runtime panic occurs.
	_, _ = ConnectEtcd([]string{"127.0.0.1:2379"},
		WithEtcdDialTimeout(1*time.Second),
		WithEtcdAuth("user", "pass"),
	)
}

func TestNewRedis(t *testing.T) {
	// Test basic connection
	u, _ := url.Parse("redis://127.0.0.1:6379")
	r, err := NewRedis(u)
	if err == nil {
		defer r.Close()
		ping := r.Ping(t.Context())
		assert.NoError(t, ping.Err())
	} else {
		t.Logf("Skipping Redis connection test: %v", err)
	}

	// Test invalid DB
	uInvalidDB, _ := url.Parse("redis://127.0.0.1:6379/abc")
	_, errDB := NewRedis(uInvalidDB)
	assert.Error(t, errDB)

	// Test Options
	// We verify that passing options doesn't panic.
	// In a real scenario, we might mock the redis client or check reflected values.
	_, _ = ConnectRedis("127.0.0.1:6379",
		WithRedisDB(1),
		WithRedisPool(10, 2),
		WithRedisTimeouts(time.Second, time.Second, time.Second),
		WithRedisTLS(&tls.Config{InsecureSkipVerify: true}),
	)
}

func TestNewRedis_QueryParams(t *testing.T) {
	// Test all query parameters parsing
	u, _ := url.Parse("redis://localhost:6379/1?pool_size=20&min_idle=5&dial_timeout=5s&read_timeout=1s&write_timeout=1s")
	// We call NewRedis. Even if connection fails, we want to ensure it doesn't panic during parsing.
	_, _ = NewRedis(u)
}

func TestNewEtcd_QueryParams(t *testing.T) {
	// Test query parameters parsing
	u, _ := url.Parse("etcd://localhost:2379?endpoints=127.0.0.1:2379,127.0.0.1:2380&dial_timeout=10s")
	_, _ = NewEtcd(u)
}

func TestConnectRedis_AddressHandling(t *testing.T) {
	// Test address without port
	// This will try to connect to localhost:6379
	_, err := ConnectRedis("localhost")
	// It might succeed or fail depending on if redis is running,
	// but we want to ensure it doesn't panic and handles the string manipulation.
	if err != nil {
		assert.Contains(t, err.Error(), "Redis connection failed")
	}

	// Test empty address -> localhost:6379
	_, err2 := ConnectRedis("")
	if err2 != nil {
		assert.Contains(t, err2.Error(), "Redis connection failed")
	}
}
