package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

type UnstructuredResource struct {
	cluster *Cluster
	def     *resourceDefinition
}

func (r *UnstructuredResource) Create(
	ctx context.Context,
	obj *Unstructured,
	opts CreateOptions,
) (*Unstructured, error) {
	if obj == nil {
		return nil, ErrInvalidObject
	}
	created := cloneUnstructured(*obj)
	if len(opts.Labels) > 0 {
		created.Metadata.Labels = cloneLabels(opts.Labels)
	}
	if len(opts.Annotations) > 0 {
		created.Metadata.Annotations = cloneAnnotations(opts.Annotations)
	}
	if len(opts.Finalizers) > 0 {
		created.Metadata.Finalizers = append([]string(nil), opts.Finalizers...)
	}
	if created.APIVersion == "" {
		created.APIVersion = r.def.APIVersion
	}
	if created.Kind == "" {
		created.Kind = r.def.Kind
	}
	if created.APIVersion != r.def.APIVersion || created.Kind != r.def.Kind {
		return nil, fmt.Errorf("%w: object does not match resource definition", ErrInvalidObject)
	}
	if err := validateObjectName(created.Metadata.Name); err != nil {
		return nil, err
	}
	name := created.Metadata.Name
	if err := r.def.defaultObject(&created); err != nil {
		return nil, err
	}
	if created.APIVersion != r.def.APIVersion || created.Kind != r.def.Kind || created.Metadata.Name != name {
		return nil, fmt.Errorf("%w: default changed object identity", ErrInvalidObject)
	}
	now := time.Now().UTC()
	uid, err := randomToken("uid", 18)
	if err != nil {
		return nil, err
	}
	created.Metadata.UID = uid
	created.Metadata.ResourceVersion = ""
	created.Metadata.Generation = 1
	created.Metadata.CreatedAt = now
	created.Metadata.UpdatedAt = now
	created.Metadata.DeletedAt = nil
	ensureMetadataMaps(&created.Metadata)
	if err := validateMetadata(created.Metadata); err != nil {
		return nil, err
	}
	if err := validateRawObjectJSON(&created); err != nil {
		return nil, err
	}
	if err := r.def.validateObject(nil, &created, SubresourceSpec); err != nil {
		return nil, err
	}
	return r.commit(ctx, commitRequest{
		Op:               commitCreate,
		Ref:              objectRef{Resource: r.def.Resource, Name: created.Metadata.Name},
		Object:           &created,
		EventType:        WatchAdded,
		EventAnnotations: opts.EventAnnotations,
		Changed:          changedPaths(nil, &created, SubresourceSpec),
	})
}

func (r *UnstructuredResource) Get(ctx context.Context, name string) (*Unstructured, error) {
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	return r.cluster.store.get(ctx, ref)
}

func (r *UnstructuredResource) List(ctx context.Context, opts ListOptions) (*UnstructuredList, error) {
	objects, rv, err := r.cluster.store.list(ctx, r.def.Resource)
	if err != nil {
		return nil, err
	}
	sortUnstructured(objects)

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	out := make([]Unstructured, 0, min(limit, len(objects)))
	started := opts.Continue == ""
	for _, obj := range objects {
		key := objectCursor(obj)
		if !started {
			started = key > opts.Continue
			if !started {
				continue
			}
		}
		if !matchesSelector(obj, opts.Selector) {
			continue
		}
		out = append(out, cloneUnstructured(obj))
		if len(out) > limit {
			return &UnstructuredList{
				Items:           out[:limit],
				ResourceVersion: formatRV(rv),
				Continue:        objectCursor(out[limit-1]),
			}, nil
		}
	}
	return &UnstructuredList{Items: out, ResourceVersion: formatRV(rv)}, nil
}

func (r *UnstructuredResource) Update(
	ctx context.Context,
	obj *Unstructured,
	opts UpdateOptions,
) (*Unstructured, error) {
	if obj == nil {
		return nil, ErrInvalidObject
	}
	input := cloneUnstructured(*obj)
	if err := validateObjectName(input.Metadata.Name); err != nil {
		return nil, err
	}
	expectedRV, err := updateRV(input.Metadata.ResourceVersion, opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	ref := objectRef{Resource: r.def.Resource, Name: input.Metadata.Name}
	oldObj, err := r.cluster.store.get(ctx, ref)
	if err != nil {
		return nil, err
	}
	if expectedRV != parseStoredRV(oldObj.Metadata.ResourceVersion) {
		return nil, ErrConflict
	}
	if !jsonEqual(input.Status, oldObj.Status) {
		return nil, fmt.Errorf("%w: status must be updated through status subresource", ErrInvalidObject)
	}
	updated, err := r.prepareSpecUpdate(*oldObj, input)
	if err != nil {
		return nil, err
	}
	return r.commit(ctx, commitRequest{
		Op:               commitUpdate,
		Ref:              ref,
		ExpectedRV:       expectedRV,
		Object:           &updated,
		EventType:        WatchModified,
		EventAnnotations: opts.EventAnnotations,
		Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
	})
}

func (r *UnstructuredResource) Patch(
	ctx context.Context,
	name string,
	patch []byte,
	opts PatchOptions,
) (*Unstructured, error) {
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	if err := validateSpecPatch(patch); err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		if expected != 0 && parseStoredRV(oldObj.Metadata.ResourceVersion) != expected {
			return nil, ErrConflict
		}
		patched, err := applyObjectPatch(*oldObj, patch)
		if err != nil {
			return nil, err
		}
		updated, err := r.prepareSpecUpdate(*oldObj, patched)
		if err != nil {
			return nil, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       parseStoredRV(oldObj.Metadata.ResourceVersion),
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) UpdateStatus(
	ctx context.Context,
	name string,
	status []byte,
	opts UpdateOptions,
) (*Unstructured, error) {
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	return r.mutateStatus(ctx, ref, expected, opts.EventAnnotations, func(obj Unstructured) (Unstructured, error) {
		obj.Status = cloneRaw(status)
		return obj, nil
	})
}

func (r *UnstructuredResource) PatchStatus(
	ctx context.Context,
	name string,
	patch []byte,
	opts PatchOptions,
) (*Unstructured, error) {
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	return r.mutateStatus(ctx, ref, expected, opts.EventAnnotations, func(obj Unstructured) (Unstructured, error) {
		raw, err := applyRawMergePatch(obj.Status, patch)
		if err != nil {
			return Unstructured{}, err
		}
		obj.Status = raw
		return obj, nil
	})
}

func (r *UnstructuredResource) Delete(ctx context.Context, name string, opts DeleteOptions) (*Unstructured, error) {
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		now := time.Now().UTC()
		updated := cloneUnstructured(*oldObj)
		updated.Metadata.DeletedAt = &now
		updated.Metadata.UpdatedAt = now
		op := commitDelete
		eventType := WatchDeleted
		if len(oldObj.Metadata.Finalizers) > 0 {
			if oldObj.Metadata.DeletedAt != nil {
				return cloneUnstructuredPtr(oldObj), nil
			}
			op = commitUpdate
			eventType = WatchModified
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               op,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        eventType,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) Watch(
	ctx context.Context,
	opts WatchOptions,
) (<-chan UnstructuredWatchEvent, error) {
	if opts.Name != "" {
		if err := validateObjectName(opts.Name); err != nil {
			return nil, err
		}
	}
	startRV, err := parseOptionalRV(opts.Since)
	if err != nil {
		return nil, err
	}
	notify, cancel, err := r.cluster.store.subscribe(ctx, r.def.Resource)
	if err != nil {
		return nil, err
	}
	if opts.Since == "" && !opts.SendInitialEvents {
		_, currentRV, err := r.cluster.store.list(ctx, r.def.Resource)
		if err != nil {
			cancel()
			return nil, err
		}
		startRV = currentRV
	}

	out := make(chan UnstructuredWatchEvent, r.cluster.options.WatchBufferSize)
	go r.watchLoop(ctx, opts, startRV, notify, cancel, out)
	return out, nil
}

func (r *UnstructuredResource) mutateStatus(
	ctx context.Context,
	ref objectRef,
	expected uint64,
	eventAnnotations Annotations,
	mutate func(Unstructured) (Unstructured, error),
) (*Unstructured, error) {
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		updated, err := mutate(cloneUnstructured(*oldObj))
		if err != nil {
			return nil, err
		}
		updated.APIVersion = oldObj.APIVersion
		updated.Kind = oldObj.Kind
		updated.Metadata = cloneMetadata(oldObj.Metadata)
		updated.Metadata.UpdatedAt = time.Now().UTC()
		if err := validateRawJSONField("status", updated.Status); err != nil {
			return nil, err
		}
		if err := r.def.validateObject(oldObj, &updated, SubresourceStatus); err != nil {
			return nil, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: eventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceStatus),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) prepareSpecUpdate(oldObj, input Unstructured) (Unstructured, error) {
	if input.APIVersion != oldObj.APIVersion || input.Kind != oldObj.Kind || input.Metadata.Name != oldObj.Metadata.Name {
		return Unstructured{}, fmt.Errorf("%w: apiVersion, kind, and name are immutable", ErrInvalidObject)
	}
	if input.Metadata.UID != "" && input.Metadata.UID != oldObj.Metadata.UID {
		return Unstructured{}, fmt.Errorf("%w: uid is immutable", ErrInvalidObject)
	}
	if input.Metadata.CreatedAt.IsZero() {
		input.Metadata.CreatedAt = oldObj.Metadata.CreatedAt
	}
	if !input.Metadata.CreatedAt.Equal(oldObj.Metadata.CreatedAt) {
		return Unstructured{}, fmt.Errorf("%w: createdAt is immutable", ErrInvalidObject)
	}
	if input.Metadata.DeletedAt != nil && oldObj.Metadata.DeletedAt == nil {
		return Unstructured{}, fmt.Errorf("%w: deletedAt is managed by Delete", ErrInvalidObject)
	}
	if input.Metadata.DeletedAt == nil && oldObj.Metadata.DeletedAt != nil {
		return Unstructured{}, fmt.Errorf("%w: deletedAt is immutable", ErrInvalidObject)
	}
	updatedAt := time.Now().UTC()
	updated := cloneUnstructured(input)
	restoreManagedFields := func() {
		updated.APIVersion = oldObj.APIVersion
		updated.Kind = oldObj.Kind
		updated.Metadata.Name = oldObj.Metadata.Name
		updated.Metadata.UID = oldObj.Metadata.UID
		updated.Metadata.ResourceVersion = oldObj.Metadata.ResourceVersion
		updated.Metadata.CreatedAt = oldObj.Metadata.CreatedAt
		updated.Metadata.UpdatedAt = updatedAt
		updated.Metadata.DeletedAt = cloneTimePtr(oldObj.Metadata.DeletedAt)
		updated.Status = cloneRaw(oldObj.Status)
		ensureMetadataMaps(&updated.Metadata)
	}
	restoreManagedFields()
	if err := validateMetadata(updated.Metadata); err != nil {
		return Unstructured{}, err
	}
	if err := r.def.defaultObject(&updated); err != nil {
		return Unstructured{}, err
	}
	restoreManagedFields()
	if err := validateMetadata(updated.Metadata); err != nil {
		return Unstructured{}, err
	}
	if err := validateRawObjectJSON(&updated); err != nil {
		return Unstructured{}, err
	}
	if jsonEqual(updated.Spec, oldObj.Spec) {
		updated.Metadata.Generation = oldObj.Metadata.Generation
	} else {
		updated.Metadata.Generation = oldObj.Metadata.Generation + 1
	}
	if err := r.def.validateObject(&oldObj, &updated, SubresourceSpec); err != nil {
		return Unstructured{}, err
	}
	return updated, nil
}

func (r *UnstructuredResource) commit(ctx context.Context, req commitRequest) (*Unstructured, error) {
	obj, _, err := r.cluster.store.commit(ctx, req)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *UnstructuredResource) ref(name string) (objectRef, error) {
	if err := validateObjectName(name); err != nil {
		return objectRef{}, err
	}
	return objectRef{Resource: r.def.Resource, Name: name}, nil
}

func sortUnstructured(objects []Unstructured) {
	sort.Slice(objects, func(i, j int) bool {
		return objectCursor(objects[i]) < objectCursor(objects[j])
	})
}
