// Package cluster provides a small Kubernetes-style typed resource control plane.
//
// Example:
//
//	c, _ := NewClusterFromURL("memory://")
//	defer c.Close()
//
//	type WidgetSpec struct {
//		Size string `json:"size,omitempty" cluster:"required,enum=small|medium|large"`
//	}
//	type WidgetStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed"`
//	}
//
//	widgets, _ := Define(c, ResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "widgets",
//		APIVersion: "example.test/v1",
//		Kind:       "Widget",
//	})
//
//	created, _ := widgets.Create(ctx, "alpha", WidgetSpec{Size: "small"}, CreateOptions{})
//	_, _ = widgets.UpdateStatus(ctx, created.Metadata.Name, WidgetStatus{Phase: "Ready"}, UpdateOptions{
//		ResourceVersion: created.Metadata.ResourceVersion,
//	})
//
//	list, _ := widgets.List(ctx, ListOptions{
//		Selector: Where(Field("status.phase").Eq("Ready")),
//	})
//	events, _ := widgets.Watch(ctx, WatchOptions{Since: list.ResourceVersion})
//	_ = events
//	statusEvents, _ := widgets.WatchStatus(ctx, WatchOptions{Since: list.ResourceVersion})
//	_ = statusEvents
package cluster

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"gopkg.d7z.net/middleware/connects"
)

// NewClusterFromURL creates a Cluster from a backend URL.
//
// Supported schemes:
//   - memory:// and mem://
//   - badger:///path/to/db
//   - etcd://127.0.0.1:2379?prefix=app
//
// Supported query parameters:
//   - prefix: backend key prefix for badger and etcd
//   - event_retention_count: number of recent watch events to retain, default 2000
//   - watch_buffer_size: per-watch channel buffer size
//
// Example:
//
//	c, _ := NewClusterFromURL("badger:///var/lib/app/cluster?prefix=control")
//	defer c.Close()
//
//	type JobSpec struct {
//		Owner string `json:"owner,omitempty" cluster:"required,index"`
//	}
//	type JobStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"index=phase,watch"`
//	}
//
//	jobs, _ := Define(c, ResourceDef[JobSpec, JobStatus]{
//		Resource:   "jobs",
//		APIVersion: "example.test/v1",
//		Kind:       "Job",
//	})
//	_, _ = jobs.Create(ctx, "daily", JobSpec{Owner: "billing"}, CreateOptions{})
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
	options := Options{Prefix: query.Get("prefix")}
	if value := query.Get("event_retention_count"); value != "" {
		parsedValue, err := strconv.Atoi(value)
		if err != nil || parsedValue <= 0 {
			return Options{}, fmt.Errorf("%w: invalid event_retention_count", ErrInvalidConfig)
		}
		options.EventRetentionCount = parsedValue
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
