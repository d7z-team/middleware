package cluster

import (
	"context"
	"errors"
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

func TestEtcdNodeLeaseLost(t *testing.T) {
	c := newURLCluster(t, newEtcdURLFactory(t), url.Values{
		"node_lease_ttl":        {"2s"},
		"node_renew_interval":   {"100ms"},
		"event_retention_count": {"10"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, c.store.releaseNode(ctx, c.options.NodeName, c.nodeToken))
	cancel()

	deadline := time.After(3 * time.Second)
	for {
		callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.CurrentNode(callCtx)
		callCancel()
		if errors.Is(err, ErrNodeLeaseLost) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected node lease loss, last error: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
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
			if query.Get("node") == "" {
				query.Set("node", fmt.Sprintf("etcd-%d", counter))
			}
			query.Set("dial_timeout", "300ms")
			return (&url.URL{
				Scheme:   "etcd",
				Host:     "127.0.0.1:2379",
				RawQuery: query.Encode(),
			}).String()
		},
	}
}
