package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type widgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type widgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase"`
}

type clusterURLFactory struct {
	name string
	raw  func(t *testing.T, query url.Values) string
}

type cleanupErrorStore struct {
	resourceStore
	mu  sync.RWMutex
	err error
}

func (s *cleanupErrorStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *cleanupErrorStore) cleanupEvents(ctx context.Context) error {
	s.mu.RLock()
	err := s.err
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return s.resourceStore.cleanupEvents(ctx)
}

func localURLFactories() []clusterURLFactory {
	memoryCounter := 0
	badgerCounter := 0
	return []clusterURLFactory{
		{
			name: "memory",
			raw: func(t *testing.T, query url.Values) string {
				t.Helper()
				memoryCounter++
				if query.Get("node") == "" {
					query.Set("node", fmt.Sprintf("memory-%d", memoryCounter))
				}
				return (&url.URL{Scheme: "memory", RawQuery: query.Encode()}).String()
			},
		},
		{
			name: "badger",
			raw: func(t *testing.T, query url.Values) string {
				t.Helper()
				badgerCounter++
				if query.Get("node") == "" {
					query.Set("node", fmt.Sprintf("badger-%d", badgerCounter))
				}
				return (&url.URL{Scheme: "badger", Path: t.TempDir(), RawQuery: query.Encode()}).String()
			},
		},
	}
}

func TestClusterURLContractLocalBackends(t *testing.T) {
	for _, factory := range localURLFactories() {
		t.Run(factory.name, func(t *testing.T) {
			runClusterURLContract(t, factory)
		})
	}
}

func TestClusterFromURLValidation(t *testing.T) {
	_, err := NewClusterFromURL("memory://")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("unknown://?node=n1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=../x")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_lease_ttl=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_renew_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_renew_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_lease_ttl=1s&node_renew_interval=1s")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_lease_ttl=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_renew_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_renew_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_lease_ttl=1s&master_renew_interval=1s")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_history_limit=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_retention_count=-1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_retention_count=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&watch_buffer_size=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("badger://?node=n1")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestClusterNodeLeaseRequiresUniqueLocalNode(t *testing.T) {
	rawURL := "memory://?node=dup&prefix=lease-test&node_lease_ttl=2s&node_renew_interval=500ms"
	first, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)

	_, err = NewClusterFromURL(rawURL)
	require.ErrorIs(t, err, ErrNodeAlreadyExists)

	other, err := NewClusterFromURL("memory://?node=other&prefix=lease-test")
	require.NoError(t, err)
	require.NoError(t, other.Close())

	require.NoError(t, first.Close())
	second, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestClusterResourceDiscoveryClosed(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=closed-discovery")
	require.NoError(t, err)

	_, err = c.Resources()
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, err = c.Resources()
	require.ErrorIs(t, err, ErrClosed)
	_, err = c.Resource(ResourceNodes)
	require.ErrorIs(t, err, ErrClosed)
}

func TestClusterMasterHistoryLimit(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=history-limit&master_history_limit=1&master_lease_ttl=500ms&master_renew_interval=50ms&node_lease_ttl=2s&node_renew_interval=500ms")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	first, err := c.Master(ctx)
	require.NoError(t, err)
	require.True(t, first.Valid)
	require.NoError(t, c.StepDown(ctx))

	deadline := time.After(2 * time.Second)
	for {
		master, err := c.Master(ctx)
		require.NoError(t, err)
		if master.Valid && master.Term > first.Term+1 {
			history, err := c.MasterHistory(ctx, 10)
			require.NoError(t, err)
			require.Len(t, history, 1)
			require.Equal(t, masterTransitionAcquired, history[0].Reason)
			require.Equal(t, "history-limit", history[0].To)
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for master reacquire")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestClusterEventCleanupDefaults(t *testing.T) {
	c := newURLCluster(t, clusterURLFactory{
		name: "memory",
		raw: func(t *testing.T, query url.Values) string {
			t.Helper()
			query.Set("node", "retention")
			return (&url.URL{Scheme: "memory", RawQuery: query.Encode()}).String()
		},
	}, nil)
	require.Equal(t, defaultEventRetentionCount, c.options.EventRetentionCount)
	require.Equal(t, c.options.MasterRenewInterval, c.options.EventCleanupInterval)
}

func TestClusterRecordsCleanupError(t *testing.T) {
	options, err := normalizeOptions(Options{
		Prefix:               "cleanup-error",
		NodeName:             "cleanup-error",
		EventCleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	errCleanup := errors.New("cleanup failed")
	store := &cleanupErrorStore{
		resourceStore: newMemoryStore(options),
		err:           errCleanup,
	}
	c, err := newCluster(options, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, time.Second)

	c.cleanupEventsIfMaster(ctx)
	c.mu.RLock()
	gotErr := c.cleanupErr
	gotAt := c.cleanupErrAt
	c.mu.RUnlock()
	require.ErrorIs(t, gotErr, errCleanup)
	require.False(t, gotAt.IsZero())

	store.setError(nil)
	c.cleanupEventsIfMaster(ctx)
	c.mu.RLock()
	gotErr = c.cleanupErr
	gotAt = c.cleanupErrAt
	c.mu.RUnlock()
	require.NoError(t, gotErr)
	require.True(t, gotAt.IsZero())
}

func TestBadgerEventCleanupDeletesLargeBacklogInBatches(t *testing.T) {
	ctx := testContext(t, 10*time.Second)
	rawURL := (&url.URL{
		Scheme: "badger",
		Path:   t.TempDir(),
		RawQuery: url.Values{
			"node":                   {"badger-batch-cleanup"},
			"event_retention_count":  {"2"},
			"event_cleanup_interval": {"1h"},
		}.Encode(),
	}).String()
	c, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	store, ok := c.store.(*badgerStore)
	require.True(t, ok)
	widgets := defineNamespacedWidgets(t, c, "badgerbatchwidgets")
	batch, err := widgets.Namespace("batch")
	require.NoError(t, err)

	for i := 0; i < defaultEventBatchSize+16; i++ {
		_, err := batch.Create(ctx, fmt.Sprintf("item-%03d", i), widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
	}
	list, err := batch.List(ctx, ListOptions{})
	require.NoError(t, err)
	before := parseStoredRV(list.ResourceVersion) - 2
	require.Greater(t, before, uint64(defaultEventBatchSize))

	c.cleanupEventsIfMaster(ctx)

	var compacted uint64
	eventPrefixes := []string{
		store.eventPrefix(resourceScope{}),
		store.eventPrefix(resourceScope{Resource: "badgerbatchwidgets", AllNamespaces: true}),
		store.eventPrefix(resourceScope{Resource: "badgerbatchwidgets", Namespace: "batch"}),
	}
	staleByPrefix := make(map[string]int, len(eventPrefixes))
	store.mu.RLock()
	err = store.db.View(func(txn *badger.Txn) error {
		var err error
		compacted, err = store.compactedRV(txn)
		if err != nil {
			return err
		}
		for _, prefix := range eventPrefixes {
			staleByPrefix[prefix] = 0
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(prefix)
			it := txn.NewIterator(opts)
			for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
				if parseRVKey(string(it.Item().Key())) <= before {
					staleByPrefix[prefix]++
				}
			}
			it.Close()
		}
		return nil
	})
	store.mu.RUnlock()
	require.NoError(t, err)
	require.GreaterOrEqual(t, compacted, before)
	for prefix, stale := range staleByPrefix {
		require.Zero(t, stale, prefix)
	}

	waitForWatchError(t, 3*time.Second, func(ctx context.Context) (<-chan WatchEvent[widgetSpec, widgetStatus], error) {
		return batch.Watch(ctx, WatchOptions{Since: "1"})
	}, ErrResourceVersionTooOld)
}

func TestBadgerURLPersistsTypedObjects(t *testing.T) {
	ctx := testContext(t, 5*time.Second)
	rawURL := (&url.URL{
		Scheme:   "badger",
		Path:     t.TempDir(),
		RawQuery: url.Values{"prefix": {"persist"}, "node": {"disk"}}.Encode(),
	}).String()

	c, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	widgets := defineWidgets(t, c, "persistwidgets")
	created, err := widgets.Create(ctx, "disk", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Annotations: Annotations{"tenant": "t1"},
	})
	require.NoError(t, err)
	require.NoError(t, c.Close())

	reopened, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	reopenedWidgets := defineWidgets(t, reopened, "persistwidgets")
	got, err := reopenedWidgets.Get(ctx, "disk")
	require.NoError(t, err)
	require.Equal(t, created.Metadata.UID, got.Metadata.UID)
	require.Equal(t, "team-a", got.Spec.Owner)
}

func runClusterURLContract(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	t.Run("crud_status_finalizers", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "widgets")
		ctx := testContext(t, 5*time.Second)

		created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		require.NotEmpty(t, created.Metadata.ResourceVersion)
		require.EqualValues(t, 1, created.Metadata.Generation)
		require.NotEmpty(t, created.Metadata.UID)
		require.Equal(t, "default-controller", created.Metadata.Annotations["controller"])

		got, err := widgets.Get(ctx, "alpha")
		require.NoError(t, err)
		require.Equal(t, created.Metadata.UID, got.Metadata.UID)

		stale := *got
		patched, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.NoError(t, err)
		require.NotEqual(t, created.Metadata.ResourceVersion, patched.Metadata.ResourceVersion)
		require.EqualValues(t, 2, patched.Metadata.Generation)

		stale.Spec.Size = "medium"
		_, err = widgets.Update(ctx, &stale, UpdateOptions{})
		require.ErrorIs(t, err, ErrConflict)

		statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)
		require.NotEqual(t, patched.Metadata.ResourceVersion, statused.Metadata.ResourceVersion)
		require.EqualValues(t, 2, statused.Metadata.Generation)

		statused.Status.Phase = "Failed"
		_, err = widgets.Update(ctx, statused, UpdateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		list, err := widgets.List(ctx, ListOptions{
			Selector: Where(Label("app").Eq("demo"), Annotation("tenant").Eq("t1"), Field("status.phase").Eq("Ready")),
		})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		withFinalizer, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{"finalizers":["cleanup.example.test"]}`), PatchOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, withFinalizer.Metadata.Finalizers)

		deleting, err := widgets.Delete(ctx, "alpha", DeleteOptions{})
		require.NoError(t, err)
		require.NotNil(t, deleting.Metadata.DeletedAt)
		_, err = widgets.Get(ctx, "alpha")
		require.NoError(t, err)

		_, err = widgets.PatchMetadata(ctx, "alpha", []byte(`{"finalizers":[]}`), PatchOptions{})
		require.NoError(t, err)
		_, err = widgets.Delete(ctx, "alpha", DeleteOptions{})
		require.NoError(t, err)
		_, err = widgets.Get(ctx, "alpha")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("schema_annotations_and_tags", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "validatedwidgets")
		ctx := testContext(t, 5*time.Second)

		_, err := widgets.Create(ctx, "missing-tenant", widgetSpec{}, CreateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		created, err := widgets.Create(ctx, "defaulted", widgetSpec{Owner: "team-a"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		require.Equal(t, "medium", created.Spec.Size)
		require.Equal(t, "default-controller", created.Metadata.Annotations["controller"])

		_, err = widgets.PatchMetadata(ctx, "defaulted", []byte(`{"annotations":{"tenant":"t2"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.Patch(ctx, "defaulted", []byte(`{"spec":{"owner":"team-b"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.Patch(ctx, "defaulted", []byte(`{"spec":{"size":"xlarge"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.UpdateStatus(ctx, "defaulted", widgetStatus{Phase: "Broken"}, UpdateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.Create(ctx, "../escape", widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("list_pagination_and_selectors", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "listedwidgets")
		ctx := testContext(t, 5*time.Second)

		for i, name := range []string{"alpha", "beta", "gamma"} {
			tenant := "t1"
			app := "demo"
			if name == "beta" {
				tenant = "t2"
				app = "other"
			}
			_, err := widgets.Create(ctx, name, widgetSpec{Size: "small", Owner: fmt.Sprintf("team-%d", i)}, CreateOptions{
				Labels:      Labels{"app": app},
				Annotations: Annotations{"tenant": tenant},
			})
			require.NoError(t, err)
		}
		_, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)
		_, err = widgets.UpdateStatus(ctx, "gamma", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)

		page, err := widgets.List(ctx, ListOptions{Limit: 1})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		require.NotEmpty(t, page.Continue)
		next, err := widgets.List(ctx, ListOptions{Limit: 10, Continue: page.Continue})
		require.NoError(t, err)
		require.Len(t, next.Items, 2)

		selected, err := widgets.List(ctx, ListOptions{
			Selector: Where(
				Label("app").In("demo"),
				Annotation("tenant").Eq("t1"),
				Annotation("tenant").Exists(),
				Field("apiVersion").Eq("example.test/v1"),
				Field("kind").Eq("Widget"),
				Field("status.phase").Eq("Ready"),
				Field("status.phase").NotIn("Failed"),
			),
		})
		require.NoError(t, err)
		require.Len(t, selected.Items, 2)

		notBeta, err := widgets.List(ctx, ListOptions{
			Selector: Where(Label("app").NotEq("other"), Field("spec.owner").Exists()),
		})
		require.NoError(t, err)
		require.Len(t, notBeta.Items, 2)

		named, err := widgets.List(ctx, ListOptions{
			Selector: Where(Field("metadata.name").Eq("alpha")),
		})
		require.NoError(t, err)
		require.Len(t, named.Items, 1)
		require.Equal(t, "alpha", named.Items[0].Metadata.Name)
	})

	t.Run("namespaced_resources", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineNamespacedWidgets(t, c, "namespacedwidgets")
		ctx := testContext(t, 6*time.Second)

		_, err := widgets.Create(ctx, "same", widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.Get(ctx, "same")
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = c.Nodes().Namespace("team-a")
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = widgets.Namespace("../escape")
		require.ErrorIs(t, err, ErrInvalidObject)

		teamA, err := widgets.Namespace("team-a")
		require.NoError(t, err)
		teamB, err := widgets.Namespace("team-b")
		require.NoError(t, err)
		all, err := widgets.AllNamespaces()
		require.NoError(t, err)

		createdA, err := teamA.Create(ctx, "same", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		require.Equal(t, "team-a", createdA.Metadata.Namespace)
		createdB, err := teamB.Create(ctx, "same", widgetSpec{Size: "medium", Owner: "team-b"}, CreateOptions{
			Annotations: Annotations{"tenant": "t2"},
		})
		require.NoError(t, err)
		require.Equal(t, "team-b", createdB.Metadata.Namespace)

		gotA, err := teamA.Get(ctx, "same")
		require.NoError(t, err)
		require.Equal(t, createdA.Metadata.UID, gotA.Metadata.UID)
		gotB, err := teamB.Get(ctx, "same")
		require.NoError(t, err)
		require.Equal(t, createdB.Metadata.UID, gotB.Metadata.UID)

		listA, err := teamA.List(ctx, ListOptions{})
		require.NoError(t, err)
		require.Len(t, listA.Items, 1)
		require.Equal(t, "team-a", listA.Items[0].Metadata.Namespace)
		listAll, err := all.List(ctx, ListOptions{})
		require.NoError(t, err)
		require.Len(t, listAll.Items, 2)
		listBase, err := widgets.List(ctx, ListOptions{
			Selector: Where(Field("metadata.namespace").Eq("team-b")),
		})
		require.NoError(t, err)
		require.Len(t, listBase.Items, 1)
		require.Equal(t, createdB.Metadata.UID, listBase.Items[0].Metadata.UID)
		rawWidgets, err := c.Unstructured("namespacedwidgets")
		require.NoError(t, err)
		_, err = rawWidgets.Get(ctx, "same")
		require.ErrorIs(t, err, ErrInvalidObject)
		rawTeamB, err := rawWidgets.Namespace("team-b")
		require.NoError(t, err)
		rawB, err := rawTeamB.Get(ctx, "same")
		require.NoError(t, err)
		require.Equal(t, createdB.Metadata.UID, rawB.Metadata.UID)

		_, err = all.Patch(ctx, "same", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		_, err = teamA.Patch(ctx, "same", []byte(`{"metadata":{"namespace":"team-b"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
		wrongNamespace := *createdA
		wrongNamespace.Metadata.Namespace = "team-b"
		_, err = teamA.Update(ctx, &wrongNamespace, UpdateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		watchCtx := testContext(t, 5*time.Second)
		teamAEvents, err := teamA.Watch(watchCtx, WatchOptions{Since: listAll.ResourceVersion})
		require.NoError(t, err)
		allEvents, err := all.Watch(watchCtx, WatchOptions{Since: listAll.ResourceVersion})
		require.NoError(t, err)

		patchedB, err := teamB.Patch(ctx, "same", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.NoError(t, err)
		event := nextWatchEvent(t, allEvents)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, patchedB.Metadata.ResourceVersion, event.ResourceVersion)
		require.Equal(t, "team-b", event.Object.Metadata.Namespace)
		requireNoWatchEvent(t, teamAEvents, 300*time.Millisecond)

		patchedA, err := teamA.Patch(ctx, "same", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{})
		require.NoError(t, err)
		event = nextWatchEvent(t, teamAEvents)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, patchedA.Metadata.ResourceVersion, event.ResourceVersion)
		require.Equal(t, "team-a", event.Object.Metadata.Namespace)
		event = nextWatchEvent(t, allEvents)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, patchedA.Metadata.ResourceVersion, event.ResourceVersion)
		require.Equal(t, "team-a", event.Object.Metadata.Namespace)

		_, err = teamA.Delete(ctx, "same", DeleteOptions{})
		require.NoError(t, err)
		_, err = teamA.Get(ctx, "same")
		require.ErrorIs(t, err, ErrNotFound)
		gotB, err = teamB.Get(ctx, "same")
		require.NoError(t, err)
		require.Equal(t, createdB.Metadata.UID, gotB.Metadata.UID)

		info, err := c.Resource("namespacedwidgets")
		require.NoError(t, err)
		require.True(t, info.Namespaced)
	})

	t.Run("watch_selectors_annotations_changed_paths", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "watchedwidgets")
		ctx := testContext(t, 5*time.Second)

		_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		_, err = widgets.Create(ctx, "beta", widgetSpec{Size: "small", Owner: "team-b"}, CreateOptions{
			Labels:      Labels{"app": "other"},
			Annotations: Annotations{"tenant": "t2"},
		})
		require.NoError(t, err)
		_, err = widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)

		initialCtx := testContext(t, 3*time.Second)
		initialEvents, err := widgets.Watch(initialCtx, WatchOptions{
			Selector:          Where(Annotation("tenant").Eq("t1")),
			SendInitialEvents: true,
			AllowBookmarks:    true,
		})
		require.NoError(t, err)
		event := nextWatchEvent(t, initialEvents)
		require.Equal(t, WatchAdded, event.Type)
		require.Equal(t, "alpha", event.Object.Metadata.Name)
		event = nextWatchEvent(t, initialEvents)
		require.Equal(t, WatchBookmark, event.Type)

		list, err := widgets.List(ctx, ListOptions{
			Selector: Where(Annotation("tenant").Eq("t1"), Field("status.phase").Eq("Ready")),
		})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		watchCtx := testContext(t, 3*time.Second)
		events, err := widgets.Watch(watchCtx, WatchOptions{
			Since:    list.ResourceVersion,
			Name:     "alpha",
			Selector: Where(Annotation("tenant").Eq("t1"), Field("spec.size").Eq("large")),
		})
		require.NoError(t, err)

		_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{
			EventAnnotations: Annotations{"reason": "resize"},
		})
		require.NoError(t, err)
		event = nextWatchEvent(t, events)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, "resize", event.Annotations["reason"])
		require.True(t, slices.Contains(event.Changed, "spec.size"), event.Changed)
	})

	t.Run("watch_metadata_and_status_scopes", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "scopedwidgets")
		ctx := testContext(t, 5*time.Second)

		_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)

		list, err := widgets.List(ctx, ListOptions{Selector: Where(Field("metadata.name").Eq("alpha"))})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		watchCtx := testContext(t, 4*time.Second)
		metadataEvents, err := widgets.WatchMetadata(watchCtx, WatchOptions{
			Since: list.ResourceVersion,
			Name:  "alpha",
		})
		require.NoError(t, err)
		statusEvents, err := widgets.WatchStatus(watchCtx, WatchOptions{
			Since: list.ResourceVersion,
			Name:  "alpha",
		})
		require.NoError(t, err)

		_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{})
		require.NoError(t, err)
		patched, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), PatchOptions{})
		require.NoError(t, err)
		require.EqualValues(t, 2, patched.Metadata.Generation)

		event := nextWatchEvent(t, metadataEvents)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, patched.Metadata.ResourceVersion, event.ResourceVersion)
		require.True(t, slices.Contains(event.Changed, "metadata.labels"), event.Changed)

		statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)

		event = nextWatchEvent(t, statusEvents)
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, statused.Metadata.ResourceVersion, event.ResourceVersion)
		require.True(t, slices.Contains(event.Changed, "status.phase"), event.Changed)
		select {
		case event, ok := <-metadataEvents:
			require.True(t, ok, "watch channel closed")
			t.Fatalf("unexpected metadata watch event: %#v", event)
		case <-time.After(300 * time.Millisecond):
		}

		initialCtx := testContext(t, 3*time.Second)
		initialEvents, err := widgets.WatchStatus(initialCtx, WatchOptions{
			Name:              "alpha",
			SendInitialEvents: true,
		})
		require.NoError(t, err)
		event = nextWatchEvent(t, initialEvents)
		require.Equal(t, WatchAdded, event.Type)
		require.Equal(t, "alpha", event.Object.Metadata.Name)
		require.Empty(t, event.Changed)

		_, err = widgets.Watch(ctx, WatchOptions{Scope: WatchScope("invalid")})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("master_api_history_and_watch", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		ctx := testContext(t, 5*time.Second)

		master, err := c.Master(ctx)
		require.NoError(t, err)
		require.True(t, master.Valid)
		require.Equal(t, c.options.NodeName, master.Node)
		require.Equal(t, uint64(1), master.Term)

		isMaster, err := c.IsMaster(ctx)
		require.NoError(t, err)
		require.True(t, isMaster)

		history, err := c.MasterHistory(ctx, 10)
		require.NoError(t, err)
		require.Len(t, history, 1)
		require.Equal(t, masterTransitionAcquired, history[0].Reason)
		require.Equal(t, c.options.NodeName, history[0].To)

		watchCtx := testContext(t, 4*time.Second)
		events, err := c.WatchMaster(watchCtx, WatchOptions{Since: master.ResourceVersion})
		require.NoError(t, err)

		require.NoError(t, c.StepDown(ctx))
		event := nextMasterWatchEvent(t, events)
		require.Equal(t, WatchModified, event.Type)
		require.NotNil(t, event.Master)
		require.False(t, event.Master.Valid)
		require.Equal(t, master.Term+1, event.Master.Term)
		require.NotNil(t, event.Transition)
		require.Equal(t, masterTransitionReleased, event.Transition.Reason)
		require.Equal(t, c.options.NodeName, event.Transition.From)

		isMaster, err = c.IsMaster(ctx)
		require.NoError(t, err)
		require.False(t, isMaster)
		require.ErrorIs(t, c.StepDown(ctx), ErrNotMaster)

		history, err = c.MasterHistory(ctx, 1)
		require.NoError(t, err)
		require.Len(t, history, 1)
		require.Equal(t, masterTransitionReleased, history[0].Reason)

		replayCtx := testContext(t, 4*time.Second)
		replayEvents, err := c.WatchMaster(replayCtx, WatchOptions{Since: master.ResourceVersion})
		require.NoError(t, err)
		event = nextMasterWatchEvent(t, replayEvents)
		require.Equal(t, WatchModified, event.Type)
		require.NotNil(t, event.Transition)
		require.Equal(t, masterTransitionReleased, event.Transition.Reason)
	})

	t.Run("node_api_and_resource_schema", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "schemawidgets")
		ctx := testContext(t, 5*time.Second)

		node, err := c.CurrentNode(ctx)
		require.NoError(t, err)
		require.Equal(t, c.options.NodeName, node.Metadata.Name)
		require.False(t, node.Status.LeaseUntil.IsZero())

		nodes := c.Nodes()
		list, err := nodes.List(ctx, ListOptions{Selector: Where(Field("metadata.name").Eq(c.options.NodeName))})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		watchCtx := testContext(t, 4*time.Second)
		metadataEvents, err := nodes.WatchMetadata(watchCtx, WatchOptions{
			Since: list.ResourceVersion,
			Name:  c.options.NodeName,
		})
		require.NoError(t, err)

		patchedMeta, err := c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"node":"current"},"annotations":{"role":"worker"}}`), PatchOptions{})
		require.NoError(t, err)
		require.Equal(t, "worker", patchedMeta.Metadata.Annotations["role"])
		require.Equal(t, node.Metadata.Generation, patchedMeta.Metadata.Generation)

		event := nextWatchEvent(t, metadataEvents)
		require.Equal(t, WatchModified, event.Type)
		require.True(t, slices.Contains(event.Changed, "metadata.labels"), event.Changed)

		patchedSpec, err := c.PatchCurrentNodeSpec(ctx, []byte(`{"metadata":{"zone":"test"}}`), PatchOptions{})
		require.NoError(t, err)
		require.Equal(t, "test", patchedSpec.Spec.Metadata["zone"])
		_, err = c.PatchCurrentNodeSpec(ctx, []byte(` `), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		statused, err := c.UpdateCurrentNodeStatus(ctx, NodeStatus{Metadata: Annotations{"ready": "true"}}, UpdateOptions{})
		require.NoError(t, err)
		require.Equal(t, "true", statused.Status.Metadata["ready"])
		require.False(t, statused.Status.LeaseUntil.IsZero())

		patchedStatus, err := c.PatchCurrentNodeStatus(ctx, []byte(`{"metadata":{"ready":"true","zone":"test"}}`), PatchOptions{})
		require.NoError(t, err)
		require.Equal(t, "test", patchedStatus.Status.Metadata["zone"])
		require.False(t, patchedStatus.Status.LeaseUntil.IsZero())
		_, err = c.PatchCurrentNodeStatus(ctx, []byte(` `), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		resources, err := c.Resources()
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
			return info.Resource == ResourceNodes && info.Builtin
		}))
		require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
			return info.Resource == ResourceMasters && info.Builtin
		}))
		require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
			return info.Resource == "schemawidgets" && !info.Builtin
		}))

		info, err := c.Resource("schemawidgets")
		require.NoError(t, err)
		require.Equal(t, "Widget", info.Kind)
		require.True(t, slices.ContainsFunc(info.Spec, func(field FieldInfo) bool {
			return field.Path == "spec.size" && field.Required && field.Indexed
		}))
		require.True(t, slices.ContainsFunc(info.Annotations, func(rule AnnotationRule) bool {
			return rule.Key == "tenant" && rule.Required && rule.Immutable && rule.Indexed
		}))

		_, err = Define(c, ResourceDef[widgetSpec, widgetStatus]{
			Resource:   ResourceNodes,
			APIVersion: "example.test/v1",
			Kind:       "Node",
		})
		require.ErrorIs(t, err, ErrInvalidResource)
		_, err = Define(c, ResourceDef[MasterSpec, MasterStatus]{
			Resource:   ResourceMasters,
			APIVersion: "example.test/v1",
			Kind:       "Master",
		})
		require.ErrorIs(t, err, ErrInvalidResource)

		_ = widgets
	})

	t.Run("watch_replay_and_retention", func(t *testing.T) {
		c := newURLCluster(t, factory, url.Values{
			"event_retention_count":  {"2"},
			"event_cleanup_interval": {"20ms"},
			"watch_buffer_size":      {"4"},
		})
		widgets := defineWidgets(t, c, "retentionwidgets")
		ctx := testContext(t, 5*time.Second)

		_, err := widgets.Create(ctx, "one", widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		list, err := widgets.List(ctx, ListOptions{})
		require.NoError(t, err)

		watchCtx := testContext(t, 3*time.Second)
		events, err := widgets.Watch(watchCtx, WatchOptions{Since: list.ResourceVersion})
		require.NoError(t, err)

		created, err := widgets.Create(ctx, "two", widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		event := nextWatchEvent(t, events)
		require.Equal(t, WatchAdded, event.Type)
		require.Equal(t, created.Metadata.ResourceVersion, event.ResourceVersion)
		require.Equal(t, "two", event.Object.Metadata.Name)

		_, err = widgets.Patch(ctx, "two", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{})
		require.NoError(t, err)
		event = nextWatchEvent(t, events)
		require.Equal(t, WatchModified, event.Type)

		_, err = widgets.Delete(ctx, "two", DeleteOptions{})
		require.NoError(t, err)
		event = nextWatchEvent(t, events)
		require.Equal(t, WatchDeleted, event.Type)

		waitForWatchError(t, 3*time.Second, func(ctx context.Context) (<-chan WatchEvent[widgetSpec, widgetStatus], error) {
			return widgets.Watch(ctx, WatchOptions{Since: "1"})
		}, ErrResourceVersionTooOld)
	})

	t.Run("unstructured_handle", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "rawwidgets")
		ctx := testContext(t, 5*time.Second)

		created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		raw, err := c.Unstructured("rawwidgets")
		require.NoError(t, err)
		got, err := raw.Get(ctx, "alpha")
		require.NoError(t, err)
		require.Equal(t, created.Metadata.UID, got.Metadata.UID)
		require.JSONEq(t, `{"size":"small","owner":"team-a"}`, string(got.Spec))

		list, err := raw.List(ctx, ListOptions{Selector: Where(Field("metadata.uid").Eq(created.Metadata.UID))})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		watchCtx := testContext(t, 3*time.Second)
		statusEvents, err := raw.WatchStatus(watchCtx, WatchOptions{
			Since: list.ResourceVersion,
			Name:  "alpha",
		})
		require.NoError(t, err)
		statused, err := raw.PatchStatus(ctx, "alpha", []byte(`{"phase":"Ready"}`), PatchOptions{})
		require.NoError(t, err)
		require.JSONEq(t, `{"phase":"Ready"}`, string(statused.Status))
		var event UnstructuredWatchEvent
		select {
		case got, ok := <-statusEvents:
			require.True(t, ok)
			event = got
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for unstructured watch event")
		}
		require.Equal(t, WatchModified, event.Type)
		require.Equal(t, statused.Metadata.ResourceVersion, event.ResourceVersion)
		require.True(t, slices.Contains(event.Changed, "status.phase"), event.Changed)

		_, err = raw.Patch(ctx, "alpha", []byte(`{"status":{"phase":"Failed"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("defaults_do_not_break_identity_or_status_isolation", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		ctx := testContext(t, 5*time.Second)

		statusDefaults, err := Define(c, ResourceDef[widgetSpec, widgetStatus]{
			Resource:   "statusdefaults",
			APIVersion: "example.test/v1",
			Kind:       "StatusDefault",
			Default: func(obj *Object[widgetSpec, widgetStatus]) error {
				if obj.Spec.Size == "" {
					obj.Spec.Size = "medium"
				}
				obj.Status.Phase = "Failed"
				return nil
			},
		})
		require.NoError(t, err)
		created, err := statusDefaults.Create(ctx, "alpha", widgetSpec{}, CreateOptions{})
		require.NoError(t, err)
		require.Empty(t, created.Status.Phase)
		statused, err := statusDefaults.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)
		require.Equal(t, "Ready", statused.Status.Phase)
		patched, err := statusDefaults.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.NoError(t, err)
		require.Equal(t, "Ready", patched.Status.Phase)

		badDefaults, err := Define(c, ResourceDef[widgetSpec, widgetStatus]{
			Resource:   "baddefaults",
			APIVersion: "example.test/v1",
			Kind:       "BadDefault",
			Default: func(obj *Object[widgetSpec, widgetStatus]) error {
				obj.Metadata.Name = "../escape"
				return nil
			},
		})
		require.NoError(t, err)
		_, err = badDefaults.Create(ctx, "bad", widgetSpec{Size: "small"}, CreateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
	})
}

func newURLCluster(t *testing.T, factory clusterURLFactory, query url.Values) *Cluster {
	t.Helper()
	copiedQuery := url.Values{}
	for key, values := range query {
		copiedQuery[key] = append([]string(nil), values...)
	}
	c, err := NewClusterFromURL(factory.raw(t, copiedQuery))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func testContext(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

func defineWidgets(t *testing.T, c *Cluster, resource string) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	return defineWidgetResource(t, c, resource, false)
}

func defineNamespacedWidgets(t *testing.T, c *Cluster, resource string) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	return defineWidgetResource(t, c, resource, true)
}

func defineWidgetResource(t *testing.T, c *Cluster, resource string, namespaced bool) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	widgets, err := Define(c, ResourceDef[widgetSpec, widgetStatus]{
		Resource:   resource,
		APIVersion: "example.test/v1",
		Kind:       "Widget",
		Namespaced: namespaced,
		Annotations: []AnnotationRule{
			{Key: "tenant", Required: true, Immutable: true, Indexed: true},
			{Key: "controller", Indexed: true, Default: "default-controller"},
		},
		Default: func(obj *Object[widgetSpec, widgetStatus]) error {
			if obj.Spec.Size == "" {
				obj.Spec.Size = "medium"
			}
			return nil
		},
		Validate: func(oldObj, newObj *Object[widgetSpec, widgetStatus], subresource Subresource) error {
			if subresource == SubresourceStatus {
				return nil
			}
			if oldObj != nil && newObj.Metadata.Generation < oldObj.Metadata.Generation {
				return ErrInvalidObject
			}
			return nil
		},
	})
	require.NoError(t, err)
	return widgets
}

func requireNoWatchEvent[S, T any](t *testing.T, events <-chan WatchEvent[S, T], wait time.Duration) {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		t.Fatalf("unexpected watch event: %#v", event)
	case <-time.After(wait):
	}
}

func nextWatchEvent[S, T any](t *testing.T, events <-chan WatchEvent[S, T]) WatchEvent[S, T] {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return WatchEvent[S, T]{}
	}
}

func waitForWatchError[S, T any](
	t *testing.T,
	timeout time.Duration,
	open func(context.Context) (<-chan WatchEvent[S, T], error),
	target error,
) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		watchCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		events, err := open(watchCtx)
		require.NoError(t, err)
		select {
		case event, ok := <-events:
			cancel()
			require.True(t, ok)
			if event.Type == WatchError && errors.Is(event.Error, target) {
				return
			}
		case <-watchCtx.Done():
			cancel()
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for watch error: %v", target)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func nextMasterWatchEvent(t *testing.T, events <-chan MasterWatchEvent) MasterWatchEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for master watch event")
		return MasterWatchEvent{}
	}
}
