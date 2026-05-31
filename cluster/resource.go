package cluster

import (
	"context"
	"encoding/json"
)

type Resource[S, T any] struct {
	raw *UnstructuredResource
}

func (r *Resource[S, T]) Namespace(namespace string) (*Resource[S, T], error) {
	raw, err := r.raw.Namespace(namespace)
	if err != nil {
		return nil, err
	}
	return &Resource[S, T]{raw: raw}, nil
}

func (r *Resource[S, T]) AllNamespaces() (*Resource[S, T], error) {
	raw, err := r.raw.AllNamespaces()
	if err != nil {
		return nil, err
	}
	return &Resource[S, T]{raw: raw}, nil
}

func (r *Resource[S, T]) Create(ctx context.Context, name string, spec S, opts CreateOptions) (*Object[S, T], error) {
	rawSpec, err := marshalValue(spec)
	if err != nil {
		return nil, err
	}
	def := r.raw.def
	out, err := r.raw.Create(ctx, &Unstructured{
		APIVersion: def.APIVersion,
		Kind:       def.Kind,
		Metadata: Metadata{
			Namespace:   r.raw.namespace,
			Name:        name,
			Labels:      cloneLabels(opts.Labels),
			Annotations: cloneAnnotations(opts.Annotations),
			Finalizers:  append([]string(nil), opts.Finalizers...),
		},
		Spec: rawSpec,
	}, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) Get(ctx context.Context, name string) (*Object[S, T], error) {
	out, err := r.raw.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) List(ctx context.Context, opts ListOptions) (*ObjectList[S, T], error) {
	out, err := r.raw.List(ctx, opts)
	if err != nil {
		return nil, err
	}
	items := make([]Object[S, T], 0, len(out.Items))
	for i := range out.Items {
		typed, err := unstructuredToTyped[S, T](&out.Items[i])
		if err != nil {
			return nil, err
		}
		items = append(items, *typed)
	}
	return &ObjectList[S, T]{
		Items:           items,
		ResourceVersion: out.ResourceVersion,
		Continue:        out.Continue,
	}, nil
}

func (r *Resource[S, T]) Update(ctx context.Context, obj *Object[S, T], opts UpdateOptions) (*Object[S, T], error) {
	if obj == nil {
		return nil, ErrInvalidObject
	}
	rawObj, err := typedToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	out, err := r.raw.Update(ctx, rawObj, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) Patch(ctx context.Context, name string, patch []byte, opts PatchOptions) (*Object[S, T], error) {
	out, err := r.raw.Patch(ctx, name, patch, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) PatchMetadata(ctx context.Context, name string, patch []byte, opts PatchOptions) (*Object[S, T], error) {
	out, err := r.raw.PatchMetadata(ctx, name, patch, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) UpdateStatus(ctx context.Context, name string, status T, opts UpdateOptions) (*Object[S, T], error) {
	rawStatus, err := marshalValue(status)
	if err != nil {
		return nil, err
	}
	out, err := r.raw.UpdateStatus(ctx, name, rawStatus, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) PatchStatus(ctx context.Context, name string, patch []byte, opts PatchOptions) (*Object[S, T], error) {
	out, err := r.raw.PatchStatus(ctx, name, patch, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) Delete(ctx context.Context, name string, opts DeleteOptions) (*Object[S, T], error) {
	out, err := r.raw.Delete(ctx, name, opts)
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[S, T](out)
}

func (r *Resource[S, T]) Watch(ctx context.Context, opts WatchOptions) (<-chan WatchEvent[S, T], error) {
	rawEvents, err := r.raw.Watch(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := make(chan WatchEvent[S, T], r.raw.cluster.options.WatchBufferSize)
	go func() {
		defer close(out)
		for event := range rawEvents {
			typed := WatchEvent[S, T]{
				Type:            event.Type,
				ResourceVersion: event.ResourceVersion,
				Annotations:     cloneAnnotations(event.Annotations),
				Changed:         append([]string(nil), event.Changed...),
				Error:           event.Error,
			}
			if event.Object != nil {
				obj, err := unstructuredToTyped[S, T](event.Object)
				if err != nil {
					typed = WatchEvent[S, T]{
						Type:            WatchError,
						ResourceVersion: event.ResourceVersion,
						Error:           err,
					}
				} else {
					typed.Object = obj
				}
			}
			select {
			case out <- typed:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (r *Resource[S, T]) WatchMetadata(ctx context.Context, opts WatchOptions) (<-chan WatchEvent[S, T], error) {
	opts.Scope = WatchScopeMetadata
	return r.Watch(ctx, opts)
}

func (r *Resource[S, T]) WatchStatus(ctx context.Context, opts WatchOptions) (<-chan WatchEvent[S, T], error) {
	opts.Scope = WatchScopeStatus
	return r.Watch(ctx, opts)
}

func unstructuredToTyped[S, T any](obj *Unstructured) (*Object[S, T], error) {
	if obj == nil {
		return nil, nil
	}
	var spec S
	if len(obj.Spec) > 0 && string(obj.Spec) != "null" {
		if err := json.Unmarshal(obj.Spec, &spec); err != nil {
			return nil, err
		}
	}
	var status T
	if len(obj.Status) > 0 && string(obj.Status) != "null" {
		if err := json.Unmarshal(obj.Status, &status); err != nil {
			return nil, err
		}
	}
	return &Object[S, T]{
		APIVersion: obj.APIVersion,
		Kind:       obj.Kind,
		Metadata:   cloneMetadata(obj.Metadata),
		Spec:       spec,
		Status:     status,
	}, nil
}

func typedToUnstructured[S, T any](obj *Object[S, T]) (*Unstructured, error) {
	spec, err := marshalValue(obj.Spec)
	if err != nil {
		return nil, err
	}
	status, err := marshalValue(obj.Status)
	if err != nil {
		return nil, err
	}
	return &Unstructured{
		APIVersion: obj.APIVersion,
		Kind:       obj.Kind,
		Metadata:   cloneMetadata(obj.Metadata),
		Spec:       spec,
		Status:     status,
	}, nil
}

func marshalValue(value any) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return nil, nil
	}
	return json.RawMessage(raw), nil
}
