// Package cluster provides a small Kubernetes-style typed resource control plane.
//
// Example:
//
//	c, _ := NewClusterFromURL("memory://?node=worker-a")
//	defer c.Close()
//
//	type WidgetSpec struct {
//		Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
//		Owner string `json:"owner,omitempty" cluster:"immutable,index"`
//	}
//	type WidgetStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index"`
//	}
//
//	schema, _ := SchemaFrom[WidgetSpec, WidgetStatus]("example.test/v1", "Widget", false)
//	_, _ = DefineResource(c, ResourceDef{
//		Resource:   "rawwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "Widget",
//		Schema:     schema,
//	})
//
//	type WidgetSpec struct {
//		Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
//		Owner string `json:"owner,omitempty" cluster:"immutable,index"`
//	}
//	type WidgetStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index"`
//	}
//
//	widgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "widgets",
//		APIVersion: "example.test/v1",
//		Kind:       "Widget",
//	})
//
//	created, _ := widgets.Create(ctx, "alpha", WidgetSpec{Owner: "team-a"}, CreateOptions{
//		Labels:      Labels{"app": "demo"},
//		Annotations: Annotations{"team": "platform"},
//	})
//	patched, _ := widgets.Patch(ctx, created.Metadata.Name, []byte(`{"spec":{"size":"large"}}`), PatchOptions{
//		ResourceVersion: created.Metadata.ResourceVersion,
//	})
//	metaPatched, _ := widgets.PatchMetadata(ctx, patched.Metadata.Name, []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), PatchOptions{
//		ResourceVersion: patched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.UpdateStatus(ctx, metaPatched.Metadata.Name, WidgetStatus{Phase: "Ready"}, UpdateOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.PatchStatus(ctx, metaPatched.Metadata.Name, []byte(`{"phase":"Failed"}`), PatchOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.Delete(ctx, metaPatched.Metadata.Name, DeleteOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//
//	list, _ := widgets.List(ctx, ListOptions{
//		Selector: Where(
//			Field("status.phase").Eq("Ready"),
//			Field("spec.owner").Eq("team-a"),
//			Label("app").Eq("demo"),
//			Annotation("team").Eq("platform"),
//		),
//	})
//	events, _ := widgets.Watch(ctx, WatchOptions{Since: list.ResourceVersion})
//	metadataEvents, _ := widgets.WatchMetadata(ctx, WatchOptions{Since: list.ResourceVersion})
//	statusEvents, _ := widgets.WatchStatus(ctx, WatchOptions{Since: list.ResourceVersion})
//	_, _, _ = events, metadataEvents, statusEvents
//
//	guardedWidgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "guardedwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "GuardedWidget",
//		Admission: []AdmissionRule{
//			{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}},
//			{Name: "metadata-check", Operations: []AdmissionOperation{AdmissionUpdate}, Subresources: []Subresource{SubresourceMetadata}},
//		},
//	})
//	_, _ = guardedWidgets.Create(ctx, "beta", WidgetSpec{Owner: "team-b"}, CreateOptions{})
//	requests, _ := c.AdmissionRequests().Watch(ctx, WatchOptions{SendInitialEvents: true})
//	_ = requests
//	_, _ = c.ApproveAdmission(ctx, "adm_x", AdmissionDecisionOptions{
//		Rule:    "create-check",
//		Decider: "controller-a",
//		Message: "approved",
//	})
//
//	namespacedWidgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "teamwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "TeamWidget",
//		Namespaced: true,
//	})
//	teamWidgets, _ := namespacedWidgets.Namespace("team-a")
//	_, _ = teamWidgets.Create(ctx, "alpha", WidgetSpec{Owner: "team-a"}, CreateOptions{})
//	allWidgets, _ := namespacedWidgets.AllNamespaces()
//	_, _ = allWidgets.List(ctx, ListOptions{
//		Selector: Where(Field("metadata.namespace").Eq("team-a")),
//	})
//
//	info, _ := c.Resource("widgets")
//	resources, _ := c.Resources()
//	_, _ = info, resources
//
//	node, _ := c.CurrentNode(ctx)
//	nodeMeta, _ := c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"role":"worker"}}`), PatchOptions{
//		ResourceVersion: node.Metadata.ResourceVersion,
//	})
//	nodeSpec, _ := c.PatchCurrentNodeSpec(ctx, []byte(`{"metadata":{"zone":"cn-sh-1"}}`), PatchOptions{
//		ResourceVersion: nodeMeta.Metadata.ResourceVersion,
//	})
//	nodeStatus, _ := c.PatchCurrentNodeStatus(ctx, []byte(`{"metadata":{"ready":"true"}}`), PatchOptions{
//		ResourceVersion: nodeSpec.Metadata.ResourceVersion,
//	})
//	_, _ = c.UpdateCurrentNodeStatus(ctx, NodeStatus{
//		Metadata: Annotations{"ready": "true"},
//	}, UpdateOptions{
//		ResourceVersion: nodeStatus.Metadata.ResourceVersion,
//	})
//	_ = node
//
//	master, _ := c.Master(ctx)
//	isMaster, _ := c.IsMaster(ctx)
//	history, _ := c.MasterHistory(ctx, 20)
//	masterEvents, _ := c.WatchMaster(ctx, WatchOptions{Since: master.ResourceVersion})
//	_, _, _, _ = isMaster, history, masterEvents, master
//
//	rawWidgets, _ := c.Unstructured("widgets")
//	rawObj, _ := rawWidgets.Get(ctx, "alpha")
//	_ = rawObj
package cluster

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/connects"
)

// NewClusterFromURL creates a Cluster from a backend URL.
//
// Supported schemes:
//   - memory:// and mem://
//   - badger:///path/to/db?node=worker-a
//   - etcd://127.0.0.1:2379?node=worker-a&prefix=app
//
// Supported query parameters:
//   - node: required unique node name for this cluster client
//   - prefix: backend key prefix for badger and etcd
//   - node_lease_ttl: node lease TTL, default 30s
//   - node_renew_interval: node lease renew interval, default 10s, minimum 10ms
//   - master_lease_ttl: master lease TTL, default follows node_lease_ttl
//   - master_renew_interval: master lease renew interval, default follows node_renew_interval, minimum 10ms
//   - master_history_limit: number of recent master transitions to retain, default 2000
//   - event_retention_count: number of recent watch events to retain, default 2000
//   - event_cleanup_interval: master-only watch event cleanup interval, default follows master_renew_interval, minimum 10ms
//   - admission_timeout: synchronous admission wait timeout, default 30s
//   - admission_retention_count: number of terminal admission requests to retain, default 2000
//   - admission_terminal_retention: minimum terminal admission request retention duration, default 10m
//   - watch_buffer_size: per-watch channel buffer size
//
// Example:
//
//	c, _ := NewClusterFromURL("badger:///var/lib/app/cluster?node=worker-a&prefix=control")
//	defer c.Close()
//
//	type JobSpec struct {
//		Owner string `json:"owner,omitempty" cluster:"required,index"`
//	}
//	type JobStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"index"`
//	}
//
//	jobs, _ := Define(c, TypedResourceDef[JobSpec, JobStatus]{
//		Resource:   "jobs",
//		APIVersion: "example.test/v1",
//		Kind:       "Job",
//		Namespaced: true,
//	})
//	billingJobs, _ := jobs.Namespace("billing")
//	_, _ = billingJobs.Create(ctx, "daily", JobSpec{Owner: "billing"}, CreateOptions{})
func NewClusterFromURL(raw string) (*Cluster, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	options, err := clusterOptionsFromURL(parsed)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(parsed.Scheme) {
	case "memory", "mem":
		return OpenMemory(options)
	case "badger":
		return OpenBadger(parsed.Path, options)
	case "etcd":
		client, err := connects.NewEtcd(parsed)
		if err != nil {
			return nil, err
		}
		c, err := OpenEtcd(client, options)
		if err != nil {
			_ = client.Close()
			return nil, err
		}
		c.closeHook = client.Close
		return c, nil
	default:
		return nil, fmt.Errorf("%w: unsupported scheme %q", ErrInvalidConfig, parsed.Scheme)
	}
}

func clusterOptionsFromURL(parsed *url.URL) (Options, error) {
	query := parsed.Query()
	options := Options{
		Prefix:   query.Get("prefix"),
		NodeName: query.Get("node"),
	}
	if value := query.Get("node_lease_ttl"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid node_lease_ttl", ErrInvalidConfig)
		}
		options.NodeLeaseTTL = parsedValue
	}
	if value := query.Get("node_renew_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid node_renew_interval", ErrInvalidConfig)
		}
		options.NodeRenewInterval = parsedValue
	}
	if value := query.Get("master_lease_ttl"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid master_lease_ttl", ErrInvalidConfig)
		}
		options.MasterLeaseTTL = parsedValue
	}
	if value := query.Get("master_renew_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid master_renew_interval", ErrInvalidConfig)
		}
		options.MasterRenewInterval = parsedValue
	}
	if value := query.Get("master_history_limit"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid master_history_limit", ErrInvalidConfig)
		}
		options.MasterHistoryLimit = parsedValue
	}
	if value := query.Get("event_retention_count"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid event_retention_count", ErrInvalidConfig)
		}
		options.EventRetentionCount = parsedValue
	}
	if value := query.Get("event_cleanup_interval"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue < minBackgroundInterval {
			return Options{}, fmt.Errorf("%w: invalid event_cleanup_interval", ErrInvalidConfig)
		}
		options.EventCleanupInterval = parsedValue
	}
	if value := query.Get("admission_timeout"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_timeout", ErrInvalidConfig)
		}
		options.AdmissionTimeout = parsedValue
	}
	if value := query.Get("admission_retention_count"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_retention_count", ErrInvalidConfig)
		}
		options.AdmissionRetentionCount = parsedValue
	}
	if value := query.Get("admission_terminal_retention"); value != "" {
		parsedValue, err := time.ParseDuration(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid admission_terminal_retention", ErrInvalidConfig)
		}
		options.AdmissionTerminalRetention = parsedValue
	}
	if value := query.Get("watch_buffer_size"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid watch_buffer_size", ErrInvalidConfig)
		}
		options.WatchBufferSize = parsedValue
	}
	return options, nil
}
