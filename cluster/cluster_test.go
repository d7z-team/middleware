package cluster

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type widgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,watch"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type widgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase,watch"`
}

type clusterURLFactory struct {
	name string
	raw  func(t *testing.T, query url.Values) string
}

func localURLFactories() []clusterURLFactory {
	return []clusterURLFactory{
		{
			name: "memory",
			raw: func(t *testing.T, query url.Values) string {
				t.Helper()
				return (&url.URL{Scheme: "memory", RawQuery: query.Encode()}).String()
			},
		},
		{
			name: "badger",
			raw: func(t *testing.T, query url.Values) string {
				t.Helper()
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
	_, err := NewClusterFromURL("unknown://")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?event_retention_count=-1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?event_retention_count=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?watch_buffer_size=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("badger://")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestClusterDefaultEventRetention(t *testing.T) {
	ctx := testContext(t, 10*time.Second)
	c := newURLCluster(t, clusterURLFactory{
		name: "memory",
		raw: func(t *testing.T, query url.Values) string {
			t.Helper()
			return (&url.URL{Scheme: "memory", RawQuery: query.Encode()}).String()
		},
	}, nil)
	widgets := defineWidgets(t, c, "retentionwidgets")

	for i := 0; i < defaultEventRetentionCount+2; i++ {
		_, err := widgets.Create(ctx, fmt.Sprintf("item-%04d", i), widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
	}

	watchCtx := testContext(t, 3*time.Second)
	events, err := widgets.Watch(watchCtx, WatchOptions{Since: "1"})
	require.NoError(t, err)
	event := nextWatchEvent(t, events)
	require.Equal(t, WatchError, event.Type)
	require.ErrorIs(t, event.Error, ErrResourceVersionTooOld)
}

func TestBadgerURLPersistsTypedObjects(t *testing.T) {
	ctx := testContext(t, 5*time.Second)
	rawURL := (&url.URL{
		Scheme:   "badger",
		Path:     t.TempDir(),
		RawQuery: url.Values{"prefix": {"persist"}}.Encode(),
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
		require.Equal(t, "1", created.Metadata.ResourceVersion)
		require.EqualValues(t, 1, created.Metadata.Generation)
		require.NotEmpty(t, created.Metadata.UID)
		require.Equal(t, "default-controller", created.Metadata.Annotations["controller"])

		got, err := widgets.Get(ctx, "alpha")
		require.NoError(t, err)
		require.Equal(t, created.Metadata.UID, got.Metadata.UID)

		stale := *got
		patched, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.NoError(t, err)
		require.Equal(t, "2", patched.Metadata.ResourceVersion)
		require.EqualValues(t, 2, patched.Metadata.Generation)

		stale.Spec.Size = "medium"
		_, err = widgets.Update(ctx, &stale, UpdateOptions{})
		require.ErrorIs(t, err, ErrConflict)

		statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
		require.NoError(t, err)
		require.Equal(t, "3", statused.Metadata.ResourceVersion)
		require.EqualValues(t, 2, statused.Metadata.Generation)

		statused.Status.Phase = "Failed"
		_, err = widgets.Update(ctx, statused, UpdateOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)

		list, err := widgets.List(ctx, ListOptions{
			Selector: Where(Label("app").Eq("demo"), Annotation("tenant").Eq("t1"), Field("status.phase").Eq("Ready")),
		})
		require.NoError(t, err)
		require.Len(t, list.Items, 1)

		withFinalizer, err := widgets.Patch(ctx, "alpha", []byte(`{"metadata":{"finalizers":["cleanup.example.test"]}}`), PatchOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, withFinalizer.Metadata.Finalizers)

		deleting, err := widgets.Delete(ctx, "alpha", DeleteOptions{})
		require.NoError(t, err)
		require.NotNil(t, deleting.Metadata.DeletedAt)
		_, err = widgets.Get(ctx, "alpha")
		require.NoError(t, err)

		_, err = widgets.Patch(ctx, "alpha", []byte(`{"metadata":{"finalizers":[]}}`), PatchOptions{})
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

		_, err = widgets.Patch(ctx, "defaulted", []byte(`{"metadata":{"annotations":{"tenant":"t2"}}}`), PatchOptions{})
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
				Label("app").Eq("demo"),
				Annotation("tenant").Eq("t1"),
				Field("apiVersion").Eq("example.test/v1"),
				Field("kind").Eq("Widget"),
				Field("status.phase").Eq("Ready"),
			),
		})
		require.NoError(t, err)
		require.Len(t, selected.Items, 2)

		named, err := widgets.List(ctx, ListOptions{
			Selector: Where(Field("metadata.name").Eq("alpha")),
		})
		require.NoError(t, err)
		require.Len(t, named.Items, 1)
		require.Equal(t, "alpha", named.Items[0].Metadata.Name)
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
		patched, err := widgets.Patch(ctx, "alpha", []byte(`{"metadata":{"labels":{"app":"demo","tier":"frontend"}}}`), PatchOptions{})
		require.NoError(t, err)

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

	t.Run("watch_replay_and_compaction", func(t *testing.T) {
		c := newURLCluster(t, factory, url.Values{
			"event_retention_count": {"2"},
			"watch_buffer_size":     {"4"},
		})
		widgets := defineWidgets(t, c, "compactwidgets")
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

		oldCtx := testContext(t, 2*time.Second)
		oldEvents, err := widgets.Watch(oldCtx, WatchOptions{Since: "1"})
		require.NoError(t, err)
		event = nextWatchEvent(t, oldEvents)
		require.Equal(t, WatchError, event.Type)
		require.ErrorIs(t, event.Error, ErrResourceVersionTooOld)
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
	widgets, err := Define(c, ResourceDef[widgetSpec, widgetStatus]{
		Resource:   resource,
		APIVersion: "example.test/v1",
		Kind:       "Widget",
		Annotations: []AnnotationRule{
			{Key: "tenant", Required: true, Immutable: true, Indexed: true, Watch: true},
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
