package connects

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadTLSConfigFromFiles(t *testing.T) {
	cfg, err := loadTLSConfigFromFiles("", "", "", "")
	require.NoError(t, err)
	require.Nil(t, cfg)

	_, err = loadTLSConfigFromFiles("", "/tmp/cert.pem", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete tls configuration")

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, []byte("not-a-certificate"), 0o644))

	_, err = loadTLSConfigFromFiles(caFile, "", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to add ca certificate")
}

func TestConnectEtcdRequiresEndpoints(t *testing.T) {
	_, err := ConnectEtcd(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no endpoints specified")
}

func TestNewEtcdValidation(t *testing.T) {
	tests := []string{
		"etcd://localhost:2379?dial_timeout=bad",
		"etcd://localhost:2379?dial_timeout=0s",
		"etcd://localhost:2379?endpoints=, ,",
		"etcd://localhost:2379?cert-file=/tmp/cert.pem",
	}

	for _, raw := range tests {
		u, err := url.Parse(raw)
		require.NoError(t, err)

		_, err = NewEtcd(u)
		require.Error(t, err, raw)
	}
}

func TestNewRedisValidation(t *testing.T) {
	tests := []string{
		"redis://localhost:6379/abc",
		"redis://localhost:6379/-1",
		"redis://localhost:6379/0?pool_size=abc",
		"redis://localhost:6379/0?pool_size=0",
		"redis://localhost:6379/0?min_idle=-1",
		"redis://localhost:6379/0?dial_timeout=bad",
		"redis://localhost:6379/0?read_timeout=0s",
		"redis://localhost:6379/0?write_timeout=-1s",
		"redis://localhost:6379/0?ca-file=/tmp/ca.pem",
		"rediss://localhost:6379/0?insecure=true",
		"rediss://localhost:6379/0?cert-file=/tmp/cert.pem",
	}

	for _, raw := range tests {
		u, err := url.Parse(raw)
		require.NoError(t, err)

		_, err = NewRedis(u)
		require.Error(t, err, raw)
	}
}
