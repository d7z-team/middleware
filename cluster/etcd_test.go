package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
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

func TestEtcdMasterElectionSwitchesNode(t *testing.T) {
	factory := newEtcdURLFactory(t)
	query := url.Values{
		"master_lease_ttl":      {"2s"},
		"master_renew_interval": {"100ms"},
		"event_retention_count": {"20"},
		"watch_buffer_size":     {"8"},
		"prefix":                {"master-switch"},
	}
	query.Set("node", "master-a")
	rawA := factory.raw(t, query)
	query.Set("node", "master-b")
	rawB := factory.raw(t, query)

	first, err := NewClusterFromURL(rawA)
	require.NoError(t, err)
	firstClosed := false
	t.Cleanup(func() {
		if !firstClosed {
			require.NoError(t, first.Close())
		}
	})
	second, err := NewClusterFromURL(rawB)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	ctx := testContext(t, 5*time.Second)
	firstMaster, err := first.Master(ctx)
	require.NoError(t, err)
	require.True(t, firstMaster.Valid)
	require.Equal(t, "master-a", firstMaster.Node)
	isMaster, err := second.IsMaster(ctx)
	require.NoError(t, err)
	require.False(t, isMaster)

	watchCtx := testContext(t, 5*time.Second)
	events, err := second.WatchMaster(watchCtx, WatchOptions{})
	require.NoError(t, err)

	require.NoError(t, first.Close())
	firstClosed = true

	var switched MasterWatchEvent
	deadline := time.After(4 * time.Second)
	for {
		select {
		case event, ok := <-events:
			require.True(t, ok)
			if event.Master != nil && event.Master.Valid && event.Master.Node == "master-b" {
				switched = event
				goto switched
			}
		case <-deadline:
			t.Fatal("timed out waiting for master handoff")
		}
	}

switched:
	require.NotNil(t, switched.Transition)
	require.Equal(t, masterTransitionAcquired, switched.Transition.Reason)
	require.Equal(t, "master-b", switched.Transition.To)
	require.Greater(t, switched.Master.Term, firstMaster.Term)

	history, err := second.MasterHistory(ctx, 10)
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(history, func(transition MasterTransition) bool {
		return transition.Reason == masterTransitionReleased && transition.From == "master-a"
	}))
	require.True(t, slices.ContainsFunc(history, func(transition MasterTransition) bool {
		return transition.Reason == masterTransitionAcquired && transition.To == "master-b"
	}))
}

func TestEtcdEventCleanupRunsOnlyOnMaster(t *testing.T) {
	factory := newEtcdURLFactory(t)
	baseQuery := url.Values{
		"prefix":                 {"master-cleanup"},
		"event_retention_count":  {"2"},
		"master_lease_ttl":       {"2s"},
		"master_renew_interval":  {"100ms"},
		"node_lease_ttl":         {"3s"},
		"node_renew_interval":    {"100ms"},
		"watch_buffer_size":      {"8"},
		"event_cleanup_interval": {"1h"},
	}
	baseQuery.Set("node", "cleanup-master")
	rawMaster := factory.raw(t, baseQuery)
	baseQuery.Set("node", "cleanup-worker")
	baseQuery.Set("event_cleanup_interval", "50ms")
	rawWorker := factory.raw(t, baseQuery)

	master, err := NewClusterFromURL(rawMaster)
	require.NoError(t, err)
	masterClosed := false
	t.Cleanup(func() {
		if !masterClosed {
			require.NoError(t, master.Close())
		}
	})
	worker, err := NewClusterFromURL(rawWorker)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, worker.Close()) })

	ctx := testContext(t, 8*time.Second)
	info, err := master.Master(ctx)
	require.NoError(t, err)
	require.True(t, info.Valid)
	require.Equal(t, "cleanup-master", info.Node)
	isMaster, err := worker.IsMaster(ctx)
	require.NoError(t, err)
	require.False(t, isMaster)

	widgets := defineWidgets(t, worker, "cleanupwidgets")
	list, err := widgets.List(ctx, ListOptions{})
	require.NoError(t, err)
	since := list.ResourceVersion
	for i := 0; i < 4; i++ {
		_, err := widgets.Create(ctx, fmt.Sprintf("item-%d", i), widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
	}

	replayCtx, replayCancel := context.WithTimeout(context.Background(), time.Second)
	replayEvents, err := widgets.Watch(replayCtx, WatchOptions{Since: since})
	require.NoError(t, err)
	event := nextWatchEvent(t, replayEvents)
	replayCancel()
	require.NotEqual(t, WatchError, event.Type)

	require.NoError(t, master.Close())
	masterClosed = true
	deadline := time.After(4 * time.Second)
	for {
		info, err = worker.Master(ctx)
		require.NoError(t, err)
		if info.Valid && info.Node == "cleanup-worker" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for cleanup worker to become master")
		case <-time.After(50 * time.Millisecond):
		}
	}
	waitForWatchError(t, 4*time.Second, func(ctx context.Context) (<-chan WatchEvent[widgetSpec, widgetStatus], error) {
		return widgets.Watch(ctx, WatchOptions{Since: since})
	}, ErrResourceVersionTooOld)
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
			prefix := query.Get("prefix")
			if prefix == "" {
				query.Set("prefix", fmt.Sprintf("%s/%d", basePrefix, counter))
			} else if !strings.HasPrefix(prefix, basePrefix+"/") {
				query.Set("prefix", basePrefix+"/"+strings.Trim(prefix, "/"))
			} else {
				query.Set("prefix", prefix)
			}
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
