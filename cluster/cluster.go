package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Cluster struct {
	store     resourceStore
	options   Options
	closeHook func() error

	mu               sync.RWMutex
	closed           bool
	definitions      map[string]*resourceDefinition
	definitionsByGVK map[string]*resourceDefinition
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
	return &Cluster{
		store:            store,
		options:          options,
		definitions:      make(map[string]*resourceDefinition),
		definitionsByGVK: make(map[string]*resourceDefinition),
	}, nil
}

func normalizeOptions(options Options) (Options, error) {
	if options.WatchBufferSize <= 0 {
		options.WatchBufferSize = defaultWatchBufferSize
	}
	if options.EventRetentionCount < 0 {
		return Options{}, ErrInvalidConfig
	}
	if options.EventRetentionCount == 0 {
		options.EventRetentionCount = defaultEventRetentionCount
	}
	return options, nil
}

func (c *Cluster) Unstructured(resource string) (*UnstructuredResource, error) {
	def, err := c.definitionForResource(resource)
	if err != nil {
		return nil, err
	}
	return &UnstructuredResource{cluster: c, def: def}, nil
}

func (c *Cluster) Compact(ctx context.Context, beforeRV string) error {
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
	c.mu.Unlock()
	if c.closeHook == nil {
		return c.store.close()
	}
	return errors.Join(c.store.close(), c.closeHook())
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
	def, ok := c.definitions[resource]
	if !ok {
		return nil, fmt.Errorf("%w: unknown resource %q", ErrInvalidResource, resource)
	}
	return def, nil
}
