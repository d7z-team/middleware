package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Cluster struct {
	store     resourceStore
	options   Options
	closeHook func() error

	mu               sync.RWMutex
	closed           bool
	leaseLost        bool
	definitions      map[string]*resourceDefinition
	definitionsByGVK map[string]*resourceDefinition
	nodeToken        string
	nodeLeaseUntil   time.Time
	leaseCancel      context.CancelFunc
	leaseDone        chan struct{}
	nodes            *Resource[NodeSpec, NodeStatus]
}

func OpenMemory(options Options) (*Cluster, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return newCluster(options, newMemoryStore(options))
}

func OpenBadger(path string, options Options) (*Cluster, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	store, err := newBadgerStore(path, options)
	if err != nil {
		return nil, err
	}
	return newCluster(options, store)
}

func OpenEtcd(client *clientv3.Client, options Options) (*Cluster, error) {
	if client == nil {
		return nil, ErrInvalidConfig
	}
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	return newCluster(options, newEtcdStore(client, options))
}

func newCluster(options Options, store resourceStore) (*Cluster, error) {
	if store == nil {
		return nil, ErrInvalidConfig
	}
	ctx, cancel := context.WithTimeout(context.Background(), options.NodeLeaseTTL)
	defer cancel()
	token, err := store.acquireNode(ctx, options.NodeName, options.NodeLeaseTTL)
	if err != nil {
		_ = store.close()
		return nil, err
	}

	c := &Cluster{
		store:            store,
		options:          options,
		definitions:      make(map[string]*resourceDefinition),
		definitionsByGVK: make(map[string]*resourceDefinition),
		nodeToken:        token,
		nodeLeaseUntil:   time.Now().UTC().Add(options.NodeLeaseTTL),
	}
	if err := c.registerBuiltins(); err != nil {
		_ = store.releaseNode(context.Background(), options.NodeName, token)
		_ = store.close()
		return nil, err
	}
	c.startNodeRenewal()
	if _, err := c.ensureCurrentNode(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func normalizeOptions(options Options) (Options, error) {
	if invalidPathToken(options.NodeName) {
		return Options{}, fmt.Errorf("%w: invalid node", ErrInvalidConfig)
	}
	if options.WatchBufferSize <= 0 {
		options.WatchBufferSize = defaultWatchBufferSize
	}
	if options.EventRetentionCount < 0 {
		return Options{}, ErrInvalidConfig
	}
	if options.EventRetentionCount == 0 {
		options.EventRetentionCount = defaultEventRetentionCount
	}
	if options.NodeLeaseTTL < 0 || options.NodeRenewInterval < 0 {
		return Options{}, ErrInvalidConfig
	}
	if options.NodeLeaseTTL == 0 {
		options.NodeLeaseTTL = defaultNodeLeaseTTL
	}
	if options.NodeRenewInterval == 0 {
		options.NodeRenewInterval = defaultNodeRenewInterval
		if options.NodeRenewInterval >= options.NodeLeaseTTL {
			options.NodeRenewInterval = options.NodeLeaseTTL / 3
		}
	}
	if options.NodeRenewInterval <= 0 || options.NodeRenewInterval >= options.NodeLeaseTTL {
		return Options{}, ErrInvalidConfig
	}
	return options, nil
}

func (c *Cluster) Nodes() *Resource[NodeSpec, NodeStatus] {
	return c.nodes
}

func (c *Cluster) CurrentNode(ctx context.Context) (*Object[NodeSpec, NodeStatus], error) {
	if err := c.ensureActive(ctx); err != nil {
		return nil, err
	}
	return c.ensureCurrentNode(ctx)
}

func (c *Cluster) PatchCurrentNodeMetadata(ctx context.Context, patch []byte, opts PatchOptions) (*Object[NodeSpec, NodeStatus], error) {
	return c.nodes.PatchMetadata(ctx, c.options.NodeName, patch, opts)
}

func (c *Cluster) PatchCurrentNodeSpec(ctx context.Context, patch []byte, opts PatchOptions) (*Object[NodeSpec, NodeStatus], error) {
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	raw, err := json.Marshal(map[string]json.RawMessage{"spec": patch})
	if err != nil {
		return nil, err
	}
	return c.nodes.Patch(ctx, c.options.NodeName, raw, opts)
}

func (c *Cluster) UpdateCurrentNodeStatus(ctx context.Context, status NodeStatus, opts UpdateOptions) (*Object[NodeSpec, NodeStatus], error) {
	status = c.withNodeLeaseStatus(status)
	return c.nodes.UpdateStatus(ctx, c.options.NodeName, status, opts)
}

func (c *Cluster) PatchCurrentNodeStatus(ctx context.Context, patch []byte, opts PatchOptions) (*Object[NodeSpec, NodeStatus], error) {
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	out, err := c.nodes.raw.mutateStatus(ctx, objectRef{Resource: ResourceNodes, Name: c.options.NodeName}, expected, opts.EventAnnotations, func(obj Unstructured) (Unstructured, error) {
		raw, err := applyRawMergePatch(obj.Status, patch)
		if err != nil {
			return Unstructured{}, err
		}
		var status NodeStatus
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &status); err != nil {
				return Unstructured{}, err
			}
		}
		obj.Status, err = marshalValue(c.withNodeLeaseStatus(status))
		if err != nil {
			return Unstructured{}, err
		}
		return obj, nil
	})
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[NodeSpec, NodeStatus](out)
}

func (c *Cluster) Resources() ([]ResourceInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.leaseLost {
		return nil, ErrNodeLeaseLost
	}
	out := make([]ResourceInfo, 0, len(c.definitions))
	for _, def := range c.definitions {
		out = append(out, resourceInfo(def))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Resource < out[j].Resource
	})
	return out, nil
}

func (c *Cluster) Resource(resource string) (ResourceInfo, error) {
	def, err := c.definitionForResource(resource)
	if err != nil {
		return ResourceInfo{}, err
	}
	return resourceInfo(def), nil
}

func (c *Cluster) Unstructured(resource string) (*UnstructuredResource, error) {
	def, err := c.definitionForResource(resource)
	if err != nil {
		return nil, err
	}
	return &UnstructuredResource{cluster: c, def: def}, nil
}

func (c *Cluster) Compact(ctx context.Context, beforeRV string) error {
	if err := c.ensureActive(ctx); err != nil {
		return err
	}
	rv, err := parseRequiredRV(beforeRV)
	if err != nil {
		return err
	}
	return c.store.compact(ctx, rv)
}

func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	leaseLost := c.leaseLost
	cancel := c.leaseCancel
	done := c.leaseDone
	token := c.nodeToken
	nodeName := c.options.NodeName
	ttl := c.options.NodeLeaseTTL
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		if done != nil {
			<-done
		}
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), ttl)
	defer releaseCancel()
	var err error
	if !leaseLost && token != "" {
		err = errors.Join(err, c.store.releaseNode(releaseCtx, nodeName, token))
	}
	if c.closeHook == nil {
		return errors.Join(err, c.store.close())
	}
	return errors.Join(err, c.store.close(), c.closeHook())
}

func (c *Cluster) registerDefinition(def *resourceDefinition) error {
	if err := validateDefinition(def); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.leaseLost {
		return ErrNodeLeaseLost
	}
	if _, exists := c.definitions[def.Resource]; exists {
		return fmt.Errorf("%w: duplicate resource %q", ErrInvalidResource, def.Resource)
	}
	gvk := def.APIVersion + "\x00" + def.Kind
	if _, exists := c.definitionsByGVK[gvk]; exists {
		return fmt.Errorf("%w: duplicate schema %s/%s", ErrInvalidResource, def.APIVersion, def.Kind)
	}
	c.definitions[def.Resource] = def
	c.definitionsByGVK[gvk] = def
	return nil
}

func (c *Cluster) definitionForResource(resource string) (*resourceDefinition, error) {
	if err := validateResourceName(resource); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.leaseLost {
		return nil, ErrNodeLeaseLost
	}
	def, ok := c.definitions[resource]
	if !ok {
		return nil, fmt.Errorf("%w: unknown resource %q", ErrInvalidResource, resource)
	}
	return def, nil
}

func (c *Cluster) ensureActive(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	if c.leaseLost {
		return ErrNodeLeaseLost
	}
	return nil
}

func (c *Cluster) registerBuiltins() error {
	nodesDef, err := buildDefinition(ResourceDef[NodeSpec, NodeStatus]{
		Resource:   ResourceNodes,
		APIVersion: "cluster.d7z.net/v1",
		Kind:       "Node",
	})
	if err != nil {
		return err
	}
	nodesDef.Builtin = true
	if err := c.registerDefinition(nodesDef); err != nil {
		return err
	}
	c.nodes = &Resource[NodeSpec, NodeStatus]{
		raw: &UnstructuredResource{cluster: c, def: nodesDef},
	}
	return nil
}

func (c *Cluster) startNodeRenewal() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.leaseCancel = cancel
	c.leaseDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(c.options.NodeRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				timeout := c.options.NodeRenewInterval
				if timeout < time.Second {
					timeout = time.Second
				}
				if timeout > c.options.NodeLeaseTTL {
					timeout = c.options.NodeLeaseTTL
				}
				renewCtx, renewCancel := context.WithTimeout(context.Background(), timeout)
				err := c.store.renewNode(renewCtx, c.options.NodeName, c.nodeToken, c.options.NodeLeaseTTL)
				renewCancel()
				if err != nil {
					c.markNodeLeaseLost()
					return
				}
				c.mu.Lock()
				c.nodeLeaseUntil = time.Now().UTC().Add(c.options.NodeLeaseTTL)
				c.mu.Unlock()
			}
		}
	}()
}

func (c *Cluster) markNodeLeaseLost() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.leaseLost = true
	}
}

func (c *Cluster) ensureCurrentNode(ctx context.Context) (*Object[NodeSpec, NodeStatus], error) {
	node, err := c.nodes.Get(ctx, c.options.NodeName)
	if err == nil {
		status := c.withNodeLeaseStatus(node.Status)
		if node.Status.LeaseUntil.Equal(status.LeaseUntil) {
			return node, nil
		}
		return c.nodes.UpdateStatus(ctx, node.Metadata.Name, status, UpdateOptions{})
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	created, err := c.nodes.Create(ctx, c.options.NodeName, NodeSpec{}, CreateOptions{})
	if err != nil && !errors.Is(err, ErrAlreadyExists) {
		return nil, err
	}
	if errors.Is(err, ErrAlreadyExists) {
		created, err = c.nodes.Get(ctx, c.options.NodeName)
		if err != nil {
			return nil, err
		}
	}
	return c.nodes.UpdateStatus(ctx, created.Metadata.Name, c.withNodeLeaseStatus(created.Status), UpdateOptions{})
}

func (c *Cluster) withNodeLeaseStatus(status NodeStatus) NodeStatus {
	c.mu.RLock()
	leaseUntil := c.nodeLeaseUntil
	c.mu.RUnlock()
	status.LeaseUntil = leaseUntil
	status.UpdatedAt = time.Now().UTC()
	return status
}

func resourceInfo(def *resourceDefinition) ResourceInfo {
	return ResourceInfo{
		Resource:    def.Resource,
		APIVersion:  def.APIVersion,
		Kind:        def.Kind,
		Spec:        fieldInfos(def.specRules),
		Status:      fieldInfos(def.statusRules),
		Annotations: annotationInfos(def.annotationRules),
		Builtin:     def.Builtin,
	}
}

func fieldInfos(rules []fieldRule) []FieldInfo {
	out := make([]FieldInfo, 0, len(rules))
	for _, rule := range rules {
		out = append(out, FieldInfo{
			Path:      rule.Path,
			Required:  rule.Required,
			Immutable: rule.Immutable,
			Indexed:   rule.Indexed,
			IndexName: rule.IndexName,
			Enum:      append([]string(nil), rule.Enum...),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

func annotationInfos(rules map[string]AnnotationRule) []AnnotationRule {
	out := make([]AnnotationRule, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}
