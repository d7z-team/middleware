package cluster

import (
	"context"
	"strings"
	"time"
)

func (r *UnstructuredResource) watchLoop(
	ctx context.Context,
	opts WatchOptions,
	startRV uint64,
	notify <-chan struct{},
	cancel func(),
	out chan<- UnstructuredWatchEvent,
) {
	defer close(out)
	defer cancel()

	lastRV := startRV
	if opts.SendInitialEvents {
		objects, rv, err := r.cluster.store.list(ctx, r.def.Resource)
		if err != nil {
			sendWatchError(ctx, out, err)
			return
		}
		sortUnstructured(objects)
		for _, obj := range objects {
			if opts.Name != "" && obj.Metadata.Name != opts.Name {
				continue
			}
			if !matchesSelector(obj, opts.Selector) {
				continue
			}
			if !sendWatchEvent(ctx, out, UnstructuredWatchEvent{
				Type:            WatchAdded,
				ResourceVersion: obj.Metadata.ResourceVersion,
				Object:          cloneUnstructuredPtr(&obj),
			}) {
				return
			}
		}
		lastRV = rv
		if opts.AllowBookmarks && !sendWatchEvent(ctx, out, UnstructuredWatchEvent{
			Type:            WatchBookmark,
			ResourceVersion: formatRV(lastRV),
		}) {
			return
		}
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastBookmark := uint64(0)
	for {
		latest, ok := r.drainEvents(ctx, opts, &lastRV, out)
		if !ok {
			return
		}
		if opts.AllowBookmarks && latest > lastBookmark {
			if !sendWatchEvent(ctx, out, UnstructuredWatchEvent{
				Type:            WatchBookmark,
				ResourceVersion: formatRV(latest),
			}) {
				return
			}
			lastBookmark = latest
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-notify:
			if !ok {
				return
			}
		case <-ticker.C:
		}
	}
}

func (r *UnstructuredResource) drainEvents(
	ctx context.Context,
	opts WatchOptions,
	lastRV *uint64,
	out chan<- UnstructuredWatchEvent,
) (uint64, bool) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		sendWatchError(ctx, out, err)
		return *lastRV, false
	}
	latestSeen := *lastRV
	for {
		events, latest, err := r.cluster.store.eventsAfter(ctx, *lastRV, r.def.Resource, defaultEventBatchSize)
		if err != nil {
			sendWatchError(ctx, out, err)
			return latestSeen, false
		}
		latestSeen = latest
		if len(events) == 0 {
			if latest > *lastRV {
				*lastRV = latest
			}
			return latestSeen, true
		}
		for _, event := range events {
			rv := parseStoredRV(event.ResourceVersion)
			if rv > *lastRV {
				*lastRV = rv
			}
			if opts.Name != "" && event.Ref.Name != opts.Name {
				continue
			}
			if !watchScopeMatches(opts.Scope, event.Changed) {
				continue
			}
			if event.Object == nil || !matchesSelector(*event.Object, opts.Selector) {
				continue
			}
			if !sendWatchEvent(ctx, out, UnstructuredWatchEvent{
				Type:            event.Type,
				ResourceVersion: event.ResourceVersion,
				Object:          cloneUnstructuredPtr(event.Object),
				Annotations:     cloneAnnotations(event.Annotations),
				Changed:         append([]string(nil), event.Changed...),
			}) {
				return latestSeen, false
			}
		}
		if len(events) < defaultEventBatchSize {
			return latestSeen, true
		}
	}
}

func validateWatchScope(scope WatchScope) error {
	switch scope {
	case "", WatchScopeObject, WatchScopeMetadata, WatchScopeStatus:
		return nil
	default:
		return ErrInvalidObject
	}
}

func watchScopeMatches(scope WatchScope, changed []string) bool {
	switch scope {
	case "", WatchScopeObject:
		return true
	case WatchScopeMetadata:
		return changedPathMatches(changed, "metadata")
	case WatchScopeStatus:
		return changedPathMatches(changed, "status")
	default:
		return false
	}
}

func changedPathMatches(changed []string, prefix string) bool {
	for _, path := range changed {
		if path == prefix || strings.HasPrefix(path, prefix+".") {
			return true
		}
	}
	return false
}

func sendWatchEvent(ctx context.Context, out chan<- UnstructuredWatchEvent, event UnstructuredWatchEvent) bool {
	select {
	case out <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func sendWatchError(ctx context.Context, out chan<- UnstructuredWatchEvent, err error) {
	_ = sendWatchEvent(ctx, out, UnstructuredWatchEvent{Type: WatchError, Error: err})
}
