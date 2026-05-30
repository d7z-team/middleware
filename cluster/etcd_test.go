package cluster

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestClusterURLContractEtcdBackend(t *testing.T) {
	runClusterURLContract(t, newEtcdURLFactory(t))
}

func newEtcdURLFactory(t *testing.T) clusterURLFactory {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Skipf("etcd unavailable: %v", err)
	}
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	basePrefix := fmt.Sprintf("cluster-url-test-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, basePrefix+"/probe", "ok"); err != nil {
		t.Skipf("etcd not reachable from test sandbox: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, basePrefix+"/", clientv3.WithPrefix())
	})

	var counter int
	return clusterURLFactory{
		name: "etcd",
		raw: func(t *testing.T, query url.Values) string {
			t.Helper()
			counter++
			query.Set("prefix", fmt.Sprintf("%s/%d", basePrefix, counter))
			query.Set("dial_timeout", "300ms")
			return (&url.URL{
				Scheme:   "etcd",
				Host:     "127.0.0.1:2379",
				RawQuery: query.Encode(),
			}).String()
		},
	}
}
