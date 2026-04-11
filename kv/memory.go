package kv

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is a thread-safe, in-memory key-value store supporting hierarchical keys.
type Memory struct {
	mu     *sync.RWMutex
	root   *node
	prefix string // Always ends with "/" unless empty. Represents the root of this view.
	store  string // Path to persistence file (only relevant for the root instance)
}

type node struct {
	values   map[string]item
	children map[string]*node
}

type item struct {
	Value    string     `json:"value"`
	TTL      *time.Time `json:"ttl,omitempty"`
	CreateAt time.Time  `json:"create_at"`
}

func newNode() *node {
	return &node{
		values:   make(map[string]item),
		children: make(map[string]*node),
	}
}

// NewMemory initializes a new memory KV.
func NewMemory(store string) (*Memory, error) {
	m := &Memory{
		mu:     &sync.RWMutex{},
		root:   newNode(),
		store:  store,
		prefix: "",
	}

	if store != "" {
		if err := m.load(); err != nil {
			if !os.IsNotExist(err) {
				if mkErr := os.MkdirAll(filepath.Dir(store), 0o755); mkErr != nil {
					return nil, mkErr
				}
			}
		}
	}
	return m, nil
}

func (m *Memory) load() error {
	data, err := os.ReadFile(m.store)
	if err != nil {
		return err
	}
	flat := make(map[string]item)
	if err := json.Unmarshal(data, &flat); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for k, v := range flat {
		if v.TTL == nil || now.Before(*v.TTL) {
			m.putLocked(k, v)
		}
	}
	return nil
}

// putLocked inserts a value into the tree. key is the full absolute key.
func (m *Memory) putLocked(fullKey string, it item) {
	fullKey = strings.Trim(fullKey, "/")
	if fullKey == "" {
		return
	}
	parts := strings.Split(fullKey, "/")
	curr := m.root
	for i := 0; i < len(parts)-1; i++ {
		dir := parts[i]
		if curr.children[dir] == nil {
			curr.children[dir] = newNode()
		}
		curr = curr.children[dir]
	}
	leaf := parts[len(parts)-1]
	curr.values[leaf] = it
}

func (m *Memory) Child(paths ...string) KV {
	newPrefix := m.prefix
	var newPrefixSb103 strings.Builder
	for _, p := range paths {
		p = strings.Trim(p, "/")
		if p != "" {
			newPrefixSb103.WriteString(p + "/")
		}
	}
	newPrefix += newPrefixSb103.String()
	return &Memory{
		mu:     m.mu,
		root:   m.root,
		prefix: newPrefix,
		store:  "",
	}
}

func (m *Memory) resolve(key string) string {
	return m.prefix + strings.Trim(key, "/")
}

func (m *Memory) find(fullKey string, createDirs bool) (*node, string) {
	fullKey = strings.Trim(fullKey, "/")
	if fullKey == "" {
		return m.root, ""
	}
	parts := strings.Split(fullKey, "/")
	curr := m.root
	for i := 0; i < len(parts)-1; i++ {
		dir := parts[i]
		next := curr.children[dir]
		if next == nil {
			if !createDirs {
				return nil, ""
			}
			next = newNode()
			curr.children[dir] = next
		}
		curr = next
	}
	return curr, parts[len(parts)-1]
}

func (m *Memory) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}

	fullKey := m.resolve(key)
	if strings.Trim(key, "/") == "" {
		return ErrInvalidKey
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	node, leaf := m.find(fullKey, true)

	now := time.Now()
	var expire *time.Time
	createAt := now

	if ttl == TTLKeep {
		if existing, ok := node.values[leaf]; ok {
			if existing.TTL == nil || now.Before(*existing.TTL) {
				expire = existing.TTL
				createAt = existing.CreateAt
			}
		}
	} else if ttl > 0 {
		t := now.Add(ttl)
		expire = &t
	}

	node.values[leaf] = item{
		Value:    value,
		TTL:      expire,
		CreateAt: createAt,
	}
	return nil
}

func (m *Memory) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	fullKey := m.resolve(key)
	m.mu.RLock()
	defer m.mu.RUnlock()

	node, leaf := m.find(fullKey, false)
	if node == nil {
		return "", ErrKeyNotFound
	}

	it, ok := node.values[leaf]
	if !ok {
		return "", ErrKeyNotFound
	}
	if it.TTL != nil && time.Now().After(*it.TTL) {
		return "", ErrKeyNotFound
	}
	return it.Value, nil
}

func (m *Memory) Delete(ctx context.Context, key string) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	fullKey := m.resolve(key)
	m.mu.Lock()
	defer m.mu.Unlock()

	node, leaf := m.find(fullKey, false)
	if node == nil {
		return false, nil
	}
	if _, ok := node.values[leaf]; ok {
		delete(node.values, leaf)
		return true, nil
	}
	return false, nil
}

func (m *Memory) DeleteAll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// DeleteAll deletes everything under m.prefix.
	// m.prefix is a directory path.
	m.mu.Lock()
	defer m.mu.Unlock()

	targetDir := strings.Trim(m.prefix, "/")
	if targetDir == "" {
		// Root
		m.root = newNode()
		return nil
	}

	// Find parent of targetDir to delete the entry
	parts := strings.Split(targetDir, "/")
	parent := m.root
	for i := 0; i < len(parts)-1; i++ {
		parent = parent.children[parts[i]]
		if parent == nil {
			return nil // Already empty
		}
	}
	leafDir := parts[len(parts)-1]

	// We want to delete the subtree, but we need to keep the node structure if it's a Child view?
	// Usually DeleteAll clears content.
	// If we just delete child from parent, the Child instance still points to the same root,
	// but the path is gone. Future Puts will recreate it. Correct.

	delete(parent.children, leafDir)
	return nil
}

func (m *Memory) Count(ctx context.Context) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	targetDir := strings.Trim(m.prefix, "/")
	node := m.root

	// Traverse to the prefix root
	if targetDir != "" {
		parts := strings.Split(targetDir, "/")
		for _, p := range parts {
			node = node.children[p]
			if node == nil {
				return 0, nil
			}
		}
	}

	return m.countRecursive(node), nil
}

func (m *Memory) countRecursive(n *node) int64 {
	var c int64
	now := time.Now()
	for _, v := range n.values {
		if v.TTL == nil || now.Before(*v.TTL) {
			c++
		}
	}
	for _, child := range n.children {
		c += m.countRecursive(child)
	}
	return c
}

func (m *Memory) Scan(ctx context.Context, opts ScanOptions) (*ScanResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}

	pairs, err := m.List(ctx, opts.Prefix)
	if err != nil {
		return nil, err
	}

	start := listCursorStartIndex(pairs, opts.Cursor)
	end := start + opts.Limit
	if end > len(pairs) {
		end = len(pairs)
	}

	res := pairs[start:end]
	var next string
	if end < len(pairs) {
		next = res[len(res)-1].Key
	}

	return &ScanResponse{
		Pairs:      res,
		NextCursor: next,
		HasMore:    end < len(pairs),
	}, nil
}

func (m *Memory) List(ctx context.Context, prefix string) ([]Pair, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	fullPrefix := m.prefix + strings.TrimLeft(prefix, "/")
	searchPath := strings.Trim(fullPrefix, "/")

	var parts []string
	if searchPath != "" {
		parts = strings.Split(searchPath, "/")
	}

	node := m.root
	depth := 0
	for _, p := range parts {
		if next := node.children[p]; next != nil {
			node = next
			depth++
		} else {
			break
		}
	}

	var res []Pair
	if depth == len(parts) {
		base := ""
		if len(parts) > 0 {
			base = strings.Join(parts, "/") + "/"
		}
		m.collect(node, base, &res)
	} else if depth == len(parts)-1 {
		match := parts[len(parts)-1]
		base := ""
		if depth > 0 {
			base = strings.Join(parts[:depth], "/") + "/"
		}
		m.collectMatch(node, base, match, &res)
	} else {
		return []Pair{}, nil
	}

	finalRes := make([]Pair, 0, len(res))
	prefixLen := len(m.prefix)

	for _, p := range res {
		if strings.HasPrefix(p.Key, m.prefix) {
			p.Key = p.Key[prefixLen:]
			finalRes = append(finalRes, p)
		}
	}

	sort.Slice(finalRes, func(i, j int) bool {
		return finalRes[i].Key < finalRes[j].Key
	})

	return finalRes, nil
}

func (m *Memory) collect(n *node, basePath string, res *[]Pair) {
	now := time.Now()
	for k, v := range n.values {
		if v.TTL == nil || now.Before(*v.TTL) {
			*res = append(*res, Pair{Key: basePath + k, Value: v.Value})
		}
	}
	for k, child := range n.children {
		m.collect(child, basePath+k+"/", res)
	}
}

func (m *Memory) collectMatch(n *node, basePath, match string, res *[]Pair) {
	now := time.Now()
	for k, v := range n.values {
		if strings.HasPrefix(k, match) {
			if v.TTL == nil || now.Before(*v.TTL) {
				*res = append(*res, Pair{Key: basePath + k, Value: v.Value})
			}
		}
	}
	for k, child := range n.children {
		if strings.HasPrefix(k, match) {
			m.collect(child, basePath+k+"/", res)
		}
	}
}

func (m *Memory) ListCurrent(ctx context.Context, prefix string) ([]Pair, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	fullPrefix := m.prefix + strings.TrimLeft(prefix, "/")
	searchPath := strings.Trim(fullPrefix, "/")

	var parts []string
	if searchPath != "" {
		parts = strings.Split(searchPath, "/")
	}

	node := m.root
	depth := 0
	for _, p := range parts {
		if next := node.children[p]; next != nil {
			node = next
			depth++
		} else {
			break
		}
	}

	var res []Pair
	now := time.Now()

	add := func(k, val string) {
		pathStr := ""
		if depth > 0 {
			pathStr = strings.Join(parts[:depth], "/") + "/"
		}

		absKey := pathStr + k
		if strings.HasPrefix(absKey, m.prefix) {
			res = append(res, Pair{Key: absKey[len(m.prefix):], Value: val})
		}
	}

	if depth == len(parts) {
		for k, v := range node.values {
			if v.TTL == nil || now.Before(*v.TTL) {
				add(k, v.Value)
			}
		}
	} else if depth == len(parts)-1 {
		match := parts[len(parts)-1]
		for k, v := range node.values {
			if strings.HasPrefix(k, match) {
				if v.TTL == nil || now.Before(*v.TTL) {
					add(k, v.Value)
				}
			}
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	return res, nil
}

func (m *Memory) PutBatch(ctx context.Context, pairs []Pair, ttl time.Duration) error {
	if invalidTTL(ttl) {
		return ErrInvalidTTL
	}
	for _, p := range pairs {
		if err := m.Put(ctx, p.Key, p.Value, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) GetBatch(ctx context.Context, keys []string) ([]string, error) {
	res := make([]string, len(keys))
	for i, k := range keys {
		v, err := m.Get(ctx, k)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}
	return res, nil
}

func (m *Memory) DeleteBatch(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if _, err := m.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) Sync() error {
	if m.store == "" {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	flat := make(map[string]item)
	m.flatten(m.root, "", flat)

	data, err := json.Marshal(flat)
	if err != nil {
		return err
	}
	return os.WriteFile(m.store, data, 0o644)
}

func (m *Memory) flatten(n *node, prefix string, res map[string]item) {
	for k, v := range n.values {
		res[prefix+k] = v
	}
	for k, child := range n.children {
		m.flatten(child, prefix+k+"/", res)
	}
}

func (m *Memory) ListCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	normalized := normalizeListOptions(opts)
	res, err := m.Scan(ctx, ScanOptions{Cursor: normalized.Cursor, Limit: int(normalized.Limit)})
	if err != nil {
		return nil, err
	}
	return &ListResponse{Pairs: res.Pairs, Cursor: res.NextCursor, HasMore: res.HasMore}, nil
}

func (m *Memory) ListCurrentCursor(ctx context.Context, opts *ListOptions) (*ListResponse, error) {
	all, err := m.ListCurrent(ctx, "")
	if err != nil {
		return nil, err
	}
	normalized := normalizeListOptions(opts)
	start := listCursorStartIndex(all, normalized.Cursor)
	limit := int(normalized.Limit)
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	return &ListResponse{
		Pairs: all[start:end],
		Cursor: func() string {
			if end < len(all) {
				return all[end-1].Key
			}
			return ""
		}(),
		HasMore: end < len(all),
	}, nil
}

func (m *Memory) ListPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, _ := m.List(ctx, prefix)
	s, e := listPageRange(len(all), pageIndex, pageSize)
	return all[s:e], nil
}

func (m *Memory) ListCurrentPage(ctx context.Context, prefix string, pageIndex uint64, pageSize uint) ([]Pair, error) {
	all, _ := m.ListCurrent(ctx, prefix)
	s, e := listPageRange(len(all), pageIndex, pageSize)
	return all[s:e], nil
}

func (m *Memory) PutIfNotExists(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if invalidTTL(ttl) {
		return false, ErrInvalidTTL
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fullKey := m.resolve(key)
	node, leaf := m.find(fullKey, true)

	if v, ok := node.values[leaf]; ok && (v.TTL == nil || time.Now().Before(*v.TTL)) {
		return false, nil
	}

	now := time.Now()
	var expire *time.Time
	if ttl > 0 {
		t := now.Add(ttl)
		expire = &t
	}
	node.values[leaf] = item{Value: value, TTL: expire, CreateAt: now}
	return true, nil
}

func (m *Memory) CompareAndSwap(ctx context.Context, key, oldValue, newValue string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fullKey := m.resolve(key)
	node, leaf := m.find(fullKey, false)
	if node == nil {
		return false, nil
	}

	v, ok := node.values[leaf]
	if !ok || (v.TTL != nil && time.Now().After(*v.TTL)) {
		return false, nil
	}
	if v.Value != oldValue {
		return false, nil
	}
	v.Value = newValue
	node.values[leaf] = v
	return true, nil
}
