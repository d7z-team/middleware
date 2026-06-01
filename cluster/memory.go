package cluster

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"
)

type memoryStore struct {
	prefix                     string
	mu                         sync.RWMutex
	objects                    map[string]map[string]Unstructured
	admissionLocks             map[string]string
	events                     []resourceEvent
	rv                         uint64
	compacted                  uint64
	retention                  int
	admissionRetention         int
	admissionTerminalRetention time.Duration
	hub                        *watchHub
	closed                     bool
}

var memoryNodeLeases = struct {
	sync.Mutex
	records map[string]nodeLeaseRecord
}{
	records: make(map[string]nodeLeaseRecord),
}

func newMemoryStore(options Options) *memoryStore {
	return &memoryStore{
		prefix:                     normalizeStorePrefix(options.Prefix),
		objects:                    make(map[string]map[string]Unstructured),
		admissionLocks:             make(map[string]string),
		events:                     make([]resourceEvent, 0),
		retention:                  options.EventRetentionCount,
		admissionRetention:         options.AdmissionRetentionCount,
		admissionTerminalRetention: options.AdmissionTerminalRetention,
		hub:                        newWatchHub(),
	}
}

func (s *memoryStore) get(ctx context.Context, ref objectRef) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	resource := s.objects[ref.Resource]
	if resource == nil {
		return nil, ErrNotFound
	}
	obj, ok := resource[objectStorageKey(ref)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneUnstructuredPtr(&obj), nil
}

func (s *memoryStore) list(ctx context.Context, scope resourceScope) ([]Unstructured, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	out := make([]Unstructured, 0)
	if scope.Resource != "" {
		for _, obj := range s.objects[scope.Resource] {
			if objectMatchesScope(obj, scope) {
				out = append(out, cloneUnstructured(obj))
			}
		}
		return out, s.rv, nil
	}
	for _, byName := range s.objects {
		for _, obj := range byName {
			if objectMatchesScope(obj, scope) {
				out = append(out, cloneUnstructured(obj))
			}
		}
	}
	return out, s.rv, nil
}

func (s *memoryStore) commit(ctx context.Context, req commitRequest) (*Unstructured, resourceEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, resourceEvent{}, err
	}
	s.mu.Lock()
	obj, event, err := s.commitLocked(req)
	s.mu.Unlock()
	if err != nil {
		return nil, resourceEvent{}, err
	}
	s.hub.notify(req.Ref)
	return obj, event, nil
}

func (s *memoryStore) admissionPending(ctx context.Context, ref objectRef) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return "", ErrClosed
	}
	return s.admissionLocks[s.admissionLockKey(ref)], nil
}

func (s *memoryStore) beginAdmission(ctx context.Context, req beginAdmissionRequest) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	lockKey := s.admissionLockKey(req.Target)
	if existing := s.admissionLocks[lockKey]; existing != "" {
		return nil, ErrAdmissionPending
	}
	now := time.Now().UTC()
	uid, err := randomToken("uid")
	if err != nil {
		return nil, err
	}
	req.Request.Metadata.UID = uid
	req.Request.Metadata.ResourceVersion = ""
	req.Request.Metadata.Generation = 1
	req.Request.Metadata.CreatedAt = now
	req.Request.Metadata.UpdatedAt = now
	obj, _, err := s.commitLocked(commitRequest{
		Op:                commitCreate,
		Ref:               objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name},
		SkipAdmissionLock: true,
		Object:            req.Request,
		EventType:         WatchAdded,
		Changed:           []string{"spec", "status"},
	})
	if err != nil {
		return nil, err
	}
	s.admissionLocks[lockKey] = req.Request.Metadata.Name
	s.hub.notify(objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name})
	return obj, nil
}

func (s *memoryStore) approveAdmission(ctx context.Context, req approveAdmissionRequest) (*Unstructured, *Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, ErrClosed
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	current, ok := s.getObjectLocked(requestRef)
	if !ok {
		return nil, nil, ErrNotFound
	}
	spec, status, err := decodeAdmissionRequest(current)
	if err != nil {
		return nil, nil, err
	}
	if status.Phase != AdmissionPendingPhase {
		return nil, cloneUnstructuredPtr(&current), nil
	}
	if req.RequireRule != "" && !slices.Contains(spec.Rules, req.RequireRule) {
		return nil, nil, ErrInvalidObject
	}
	if req.Decision.Rule == "" {
		req.Decision.Rule = req.RequireRule
	}
	if req.Decision.Rule == "" {
		return nil, nil, ErrInvalidObject
	}
	for _, decision := range status.Approved {
		if decision.Rule == req.Decision.Rule {
			return nil, cloneUnstructuredPtr(&current), nil
		}
	}
	now := time.Now().UTC()
	status.Approved = append(status.Approved, AdmissionRuleDecision{
		Rule:    req.Decision.Rule,
		Message: req.Decision.Message,
		Decider: req.Decision.Decider,
		At:      now,
	})
	targetRef := objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}
	lockKey := lockKeyFromRef(targetRef)
	if len(status.Approved) < len(spec.Rules) {
		status.Phase = AdmissionPendingPhase
		updatedReq, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return nil, nil, err
		}
		out, _, err := s.commitLocked(commitRequest{
			Op:                commitUpdate,
			Ref:               requestRef,
			ExpectedRV:        parseStoredRV(current.Metadata.ResourceVersion),
			SkipAdmissionLock: true,
			Object:            updatedReq,
			EventType:         WatchModified,
			Changed:           []string{"status.approved"},
		})
		if err == nil {
			s.hub.notify(requestRef)
		}
		return nil, out, err
	}
	targetCommit, targetObj, err := admissionTargetCommit(spec)
	if err != nil {
		return nil, nil, err
	}
	targetCommit.SkipAdmissionLock = true
	targetOut, targetEvent, err := s.commitLocked(targetCommit)
	if err != nil {
		return nil, nil, err
	}
	status.Phase = AdmissionCommittedPhase
	status.Message = req.Decision.Message
	status.DecidedBy = req.Decision.Decider
	status.DecidedAt = now
	status.TargetResourceVersion = targetOut.Metadata.ResourceVersion
	status.TargetObject = targetObj
	updatedReq, err := encodeAdmissionRequest(current.Metadata, spec, status)
	if err != nil {
		return nil, nil, err
	}
	requestOut, requestEvent, err := s.commitLocked(commitRequest{
		Op:                commitUpdate,
		Ref:               requestRef,
		ExpectedRV:        parseStoredRV(current.Metadata.ResourceVersion),
		SkipAdmissionLock: true,
		Object:            updatedReq,
		EventType:         WatchModified,
		Changed:           []string{"status"},
	})
	if err != nil {
		return nil, nil, err
	}
	delete(s.admissionLocks, lockKey)
	s.hub.notify(targetRef)
	s.hub.notify(requestRef)
	_ = targetEvent
	_ = requestEvent
	return targetOut, requestOut, nil
}

func (s *memoryStore) rejectAdmission(ctx context.Context, req rejectAdmissionRequest) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	current, ok := s.getObjectLocked(requestRef)
	if !ok {
		return nil, ErrNotFound
	}
	spec, status, err := decodeAdmissionRequest(current)
	if err != nil {
		return nil, err
	}
	if status.Phase != AdmissionPendingPhase {
		return cloneUnstructuredPtr(&current), nil
	}
	now := time.Now().UTC()
	status.Phase = AdmissionRejectedPhase
	status.RejectedRule = req.Decision.Rule
	status.Message = req.Decision.Message
	status.DecidedBy = req.Decision.Decider
	status.DecidedAt = now
	updatedReq, err := encodeAdmissionRequest(current.Metadata, spec, status)
	if err != nil {
		return nil, err
	}
	out, _, err := s.commitLocked(commitRequest{
		Op:                commitUpdate,
		Ref:               requestRef,
		ExpectedRV:        parseStoredRV(current.Metadata.ResourceVersion),
		SkipAdmissionLock: true,
		Object:            updatedReq,
		EventType:         WatchModified,
		Changed:           []string{"status"},
	})
	if err != nil {
		return nil, err
	}
	delete(s.admissionLocks, lockKeyFromRef(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))
	s.hub.notify(requestRef)
	return out, nil
}

func (s *memoryStore) expireAdmission(ctx context.Context, name string, phase AdmissionPhase, message string) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	current, ok := s.getObjectLocked(requestRef)
	if !ok {
		return nil, ErrNotFound
	}
	spec, status, err := decodeAdmissionRequest(current)
	if err != nil {
		return nil, err
	}
	if status.Phase != AdmissionPendingPhase {
		return cloneUnstructuredPtr(&current), nil
	}
	now := time.Now().UTC()
	status.Phase = phase
	status.Message = message
	status.DecidedAt = now
	updatedReq, err := encodeAdmissionRequest(current.Metadata, spec, status)
	if err != nil {
		return nil, err
	}
	out, _, err := s.commitLocked(commitRequest{
		Op:                commitUpdate,
		Ref:               requestRef,
		ExpectedRV:        parseStoredRV(current.Metadata.ResourceVersion),
		SkipAdmissionLock: true,
		Object:            updatedReq,
		EventType:         WatchModified,
		Changed:           []string{"status"},
	})
	if err != nil {
		return nil, err
	}
	delete(s.admissionLocks, lockKeyFromRef(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))
	s.hub.notify(requestRef)
	return out, nil
}

func (s *memoryStore) commitLocked(req commitRequest) (*Unstructured, resourceEvent, error) {
	if s.closed {
		return nil, resourceEvent{}, ErrClosed
	}
	if !req.SkipAdmissionLock && req.Ref.Resource != ResourceAdmissionRequests {
		if name := s.admissionLocks[s.admissionLockKey(req.Ref)]; name != "" {
			return nil, resourceEvent{}, ErrAdmissionPending
		}
	}
	resource := s.objects[req.Ref.Resource]
	current, exists := resource[objectStorageKey(req.Ref)]
	switch req.Op {
	case commitCreate:
		if exists {
			return nil, resourceEvent{}, ErrAlreadyExists
		}
	case commitUpdate, commitDelete:
		if !exists {
			return nil, resourceEvent{}, ErrNotFound
		}
		if parseStoredRV(current.Metadata.ResourceVersion) != req.ExpectedRV {
			return nil, resourceEvent{}, ErrConflict
		}
	default:
		return nil, resourceEvent{}, ErrUnsupported
	}

	s.rv++
	eventObj := cloneUnstructured(*req.Object)
	eventObj.Metadata.ResourceVersion = formatRV(s.rv)
	if req.Op != commitDelete {
		if resource == nil {
			resource = make(map[string]Unstructured)
			s.objects[req.Ref.Resource] = resource
		}
		resource[objectStorageKey(req.Ref)] = eventObj
	} else {
		delete(resource, objectStorageKey(req.Ref))
		if len(resource) == 0 {
			delete(s.objects, req.Ref.Resource)
		}
	}
	event := newStoreEvent(req, s.rv, &eventObj)
	s.events = append(s.events, event)
	return cloneUnstructuredPtr(&eventObj), event, nil
}

func (s *memoryStore) getObjectLocked(ref objectRef) (Unstructured, bool) {
	resource := s.objects[ref.Resource]
	if resource == nil {
		return Unstructured{}, false
	}
	obj, ok := resource[objectStorageKey(ref)]
	return obj, ok
}

func (s *memoryStore) eventsAfter(ctx context.Context, after uint64, scope resourceScope, limit int) ([]resourceEvent, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	if after < s.compacted {
		return nil, s.rv, ErrResourceVersionTooOld
	}
	out := make([]resourceEvent, 0, min(limit, len(s.events)))
	for _, event := range s.events {
		if parseStoredRV(event.ResourceVersion) <= after {
			continue
		}
		if !eventMatchesScope(event, scope) {
			continue
		}
		out = append(out, cloneEvent(event))
		if len(out) >= limit {
			break
		}
	}
	return out, s.rv, nil
}

func (s *memoryStore) cleanupEvents(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.expireAndCleanupAdmissionsLocked(time.Now().UTC()); err != nil {
		return err
	}
	s.enforceRetentionLocked()
	return nil
}

func (s *memoryStore) subscribe(ctx context.Context, scope resourceScope) (<-chan struct{}, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.hub.subscribe(scope)
}

func (s *memoryStore) acquireNode(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token, err := randomToken("node")
	if err != nil {
		return "", err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	now := time.Now().UTC()
	if record, ok := memoryNodeLeases.records[key]; ok {
		if record.ExpiresAt.After(now) {
			return "", ErrNodeAlreadyExists
		}
		delete(memoryNodeLeases.records, key)
	}
	memoryNodeLeases.records[key] = nodeLeaseRecord{
		Token:     token,
		ExpiresAt: now.Add(ttl),
	}
	return token, nil
}

func (s *memoryStore) renewNode(ctx context.Context, name, token string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	record, ok := memoryNodeLeases.records[key]
	now := time.Now().UTC()
	if !ok || record.Token != token || !record.ExpiresAt.After(now) {
		return ErrNodeLeaseLost
	}
	record.ExpiresAt = now.Add(ttl)
	memoryNodeLeases.records[key] = record
	return nil
}

func (s *memoryStore) releaseNode(ctx context.Context, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memoryNodeLeases.Lock()
	defer memoryNodeLeases.Unlock()
	key := s.nodeLeaseKey(name)
	if record, ok := memoryNodeLeases.records[key]; ok && record.Token == token {
		delete(memoryNodeLeases.records, key)
	}
	return nil
}

func (s *memoryStore) close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.hub.close()
	return nil
}

func (s *memoryStore) nodeLeaseKey(name string) string {
	return s.prefix + "leases/nodes/" + name
}

func (s *memoryStore) admissionLockKey(ref objectRef) string {
	return lockKeyFromRef(ref)
}

func (s *memoryStore) enforceRetentionLocked() {
	if s.rv <= uint64(s.retention) {
		return
	}
	before := s.rv - uint64(s.retention)
	s.compacted = max(s.compacted, before)
	kept := s.events[:0]
	for _, event := range s.events {
		if parseStoredRV(event.ResourceVersion) > before {
			kept = append(kept, event)
		}
	}
	s.events = kept
}

func (s *memoryStore) expireAndCleanupAdmissionsLocked(now time.Time) error {
	resource := s.objects[ResourceAdmissionRequests]
	if len(resource) == 0 {
		return nil
	}
	terminal := make([]Unstructured, 0, len(resource))
	for _, obj := range resource {
		spec, status, err := decodeAdmissionRequest(obj)
		if err != nil {
			return err
		}
		if status.Phase == AdmissionPendingPhase && !spec.ExpiresAt.IsZero() && !spec.ExpiresAt.After(now) {
			status.Phase = AdmissionExpiredPhase
			status.Message = "admission timeout"
			status.DecidedAt = now
			updated, err := encodeAdmissionRequest(obj.Metadata, spec, status)
			if err != nil {
				return err
			}
			updated.Metadata.UpdatedAt = now
			if _, _, err := s.commitLocked(commitRequest{
				Op:                commitUpdate,
				Ref:               objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name},
				ExpectedRV:        parseStoredRV(obj.Metadata.ResourceVersion),
				SkipAdmissionLock: true,
				Object:            updated,
				EventType:         WatchModified,
				Changed:           []string{"status"},
			}); err != nil {
				return err
			}
			delete(s.admissionLocks, lockKeyFromRef(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))
			s.hub.notify(objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name})
			obj = *updated
		}
		if status.Phase != AdmissionPendingPhase {
			terminal = append(terminal, obj)
		}
	}
	if len(terminal) <= s.admissionRetention {
		return nil
	}
	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].Metadata.UpdatedAt.After(terminal[j].Metadata.UpdatedAt)
	})
	for _, obj := range terminal[s.admissionRetention:] {
		if now.Sub(obj.Metadata.UpdatedAt) < s.admissionTerminalRetention {
			continue
		}
		delete(resource, objectStorageKey(objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name}))
	}
	if len(resource) == 0 {
		delete(s.objects, ResourceAdmissionRequests)
	}
	return nil
}
