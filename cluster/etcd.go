package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const etcdCommitRetries = 16

type etcdStore struct {
	client                     *clientv3.Client
	prefix                     string
	retention                  int
	admissionRetention         int
	admissionTerminalRetention time.Duration
	mu                         sync.RWMutex
	closed                     bool
}

func newEtcdStore(client *clientv3.Client, options Options) *etcdStore {
	return &etcdStore{
		client:                     client,
		prefix:                     normalizeStorePrefix(options.Prefix),
		retention:                  options.EventRetentionCount,
		admissionRetention:         options.AdmissionRetentionCount,
		admissionTerminalRetention: options.AdmissionTerminalRetention,
	}
}

func (s *etcdStore) get(ctx context.Context, ref objectRef) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	resp, err := s.client.Get(ctx, s.objectKey(ref), clientv3.WithLimit(1))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}
	var obj Unstructured
	if err := json.Unmarshal(resp.Kvs[0].Value, &obj); err != nil {
		return nil, err
	}
	return cloneUnstructuredPtr(&obj), nil
}

func (s *etcdStore) list(ctx context.Context, scope resourceScope) ([]Unstructured, uint64, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, 0, err
	}
	prefix := s.objectPrefix(scope)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, 0, err
	}
	objects := make([]Unstructured, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var obj Unstructured
		if err := json.Unmarshal(kv.Value, &obj); err != nil {
			return nil, 0, err
		}
		if objectMatchesScope(obj, scope) {
			objects = append(objects, cloneUnstructured(obj))
		}
	}
	rv, err := s.currentRV(ctx)
	if err != nil {
		return nil, 0, err
	}
	return objects, rv, nil
}

func (s *etcdStore) commit(ctx context.Context, req commitRequest) (*Unstructured, resourceEvent, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, resourceEvent{}, err
	}
	var lastErr error
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, resourceEvent{}, err
		}
		objResp, err := s.client.Get(ctx, s.objectKey(req.Ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, resourceEvent{}, err
		}
		lockResp, err := s.client.Get(ctx, s.admissionLockKey(req.Ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, resourceEvent{}, err
		}
		exists := len(objResp.Kvs) > 0
		var current Unstructured
		var currentRaw []byte
		if exists {
			currentRaw = append([]byte(nil), objResp.Kvs[0].Value...)
			if err := json.Unmarshal(currentRaw, &current); err != nil {
				return nil, resourceEvent{}, err
			}
		}
		if !req.SkipAdmissionLock && req.Ref.Resource != ResourceAdmissionRequests && len(lockResp.Kvs) > 0 {
			return nil, resourceEvent{}, ErrAdmissionPending
		}
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

		nextRV := currentRV + 1
		out := cloneUnstructured(*req.Object)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, resourceEvent{}, err
		}
		event := newStoreEvent(req, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, resourceEvent{}, err
		}

		cmps := []clientv3.Cmp{s.metaRVCompare(metaRaw, metaVersion)}
		if !req.SkipAdmissionLock && req.Ref.Resource != ResourceAdmissionRequests {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.admissionLockKey(req.Ref)), "=", 0))
		}
		if req.Op == commitCreate {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(req.Ref)), "=", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(req.Ref)), "=", string(currentRaw)))
		}
		ops := []clientv3.Op{
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.eventAllKey(nextRV), string(eventRaw)),
			clientv3.OpPut(s.eventResourceKey(req.Ref.Resource, nextRV), string(eventRaw)),
			clientv3.OpPut(s.notifyKey(resourceScope{}), formatRV(nextRV)),
			clientv3.OpPut(s.notifyKey(resourceScope{Resource: req.Ref.Resource, AllNamespaces: true}), formatRV(nextRV)),
		}
		if req.Ref.Namespace != "" {
			ops = append(ops,
				clientv3.OpPut(s.eventNamespaceKey(req.Ref, nextRV), string(eventRaw)),
				clientv3.OpPut(s.notifyKey(resourceScope{Resource: req.Ref.Resource, Namespace: req.Ref.Namespace}), formatRV(nextRV)),
			)
		}
		if req.Op == commitDelete {
			ops = append(ops, clientv3.OpDelete(s.objectKey(req.Ref)))
		} else {
			ops = append(ops, clientv3.OpPut(s.objectKey(req.Ref), string(objectRaw)))
		}

		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, resourceEvent{}, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), event, nil
		}
		lastErr = ErrConflict
		select {
		case <-ctx.Done():
			return nil, resourceEvent{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if lastErr != nil {
		return nil, resourceEvent{}, lastErr
	}
	return nil, resourceEvent{}, ErrConflict
}

func (s *etcdStore) admissionPending(ctx context.Context, ref objectRef) (string, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return "", err
	}
	resp, err := s.client.Get(ctx, s.admissionLockKey(ref), clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (s *etcdStore) beginAdmission(ctx context.Context, req beginAdmissionRequest) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	spec, _, err := decodeAdmissionRequest(*req.Request)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	token, err := randomToken("uid")
	if err != nil {
		return nil, err
	}
	req.Request.Metadata.UID = token
	req.Request.Metadata.ResourceVersion = ""
	req.Request.Metadata.Generation = 1
	req.Request.Metadata.CreatedAt = now
	req.Request.Metadata.UpdatedAt = now
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name}
	lockKey := s.admissionLockKey(req.Target)
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(lockResp.Kvs) > 0 {
			return nil, ErrAdmissionPending
		}
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(requestResp.Kvs) > 0 {
			return nil, ErrAlreadyExists
		}
		targetResp, err := s.client.Get(ctx, s.objectKey(req.Target), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		targetExists := len(targetResp.Kvs) > 0
		var target Unstructured
		var targetRaw []byte
		if targetExists {
			targetRaw = append([]byte(nil), targetResp.Kvs[0].Value...)
			if err := json.Unmarshal(targetRaw, &target); err != nil {
				return nil, err
			}
		}
		switch {
		case spec.Precondition.MustNotExist:
			if targetExists {
				return nil, ErrAlreadyExists
			}
		case spec.Precondition.MustExist:
			if !targetExists {
				return nil, ErrNotFound
			}
			if spec.Precondition.ResourceVersion != "" && target.Metadata.ResourceVersion != spec.Precondition.ResourceVersion {
				return nil, ErrConflict
			}
		}

		nextRV := currentRV + 1
		out := cloneUnstructured(*req.Request)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitCreate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchAdded,
			Changed:           []string{"spec", "status"},
		}, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		cmps := []clientv3.Cmp{
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Version(lockKey), "=", 0),
			clientv3.Compare(clientv3.Version(s.objectKey(requestRef)), "=", 0),
		}
		if spec.Precondition.MustNotExist {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(req.Target)), "=", 0))
		} else if targetExists {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(req.Target)), "=", string(targetRaw)))
		}
		ops := s.eventWriteOps(requestRef, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(requestRef), string(objectRaw)),
			clientv3.OpPut(lockKey, req.Request.Metadata.Name),
		)
		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) approveAdmission(ctx context.Context, req approveAdmissionRequest) (*Unstructured, *Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, nil, err
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, nil, err
		}
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		if len(requestResp.Kvs) == 0 {
			return nil, nil, ErrNotFound
		}
		currentRaw := append([]byte(nil), requestResp.Kvs[0].Value...)
		var current Unstructured
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			return nil, nil, err
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

		lockKey := s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name})
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		if len(lockResp.Kvs) == 0 || string(lockResp.Kvs[0].Value) != req.Name {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		now := time.Now().UTC()
		status.Approved = append(status.Approved, AdmissionRuleDecision{
			Rule:    req.Decision.Rule,
			Message: req.Decision.Message,
			Decider: req.Decision.Decider,
			At:      now,
		})
		if len(status.Approved) < len(spec.Rules) {
			updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
			if err != nil {
				return nil, nil, err
			}
			requestOut, err := s.updateAdmissionRequestTxn(ctx, requestRef, currentRaw, metaRaw, metaVersion, updated, []string{"status.approved"})
			if err != nil {
				if errors.Is(err, ErrConflict) {
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					case <-time.After(10 * time.Millisecond):
						continue
					}
				}
				return nil, nil, err
			}
			return nil, requestOut, nil
		}

		targetCommit, targetObj, err := admissionTargetCommit(spec)
		if err != nil {
			return nil, nil, err
		}
		targetCommit.SkipAdmissionLock = true
		targetResp, err := s.client.Get(ctx, s.objectKey(targetCommit.Ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		targetExists := len(targetResp.Kvs) > 0
		var targetCurrent Unstructured
		var targetCurrentRaw []byte
		if targetExists {
			targetCurrentRaw = append([]byte(nil), targetResp.Kvs[0].Value...)
			if err := json.Unmarshal(targetCurrentRaw, &targetCurrent); err != nil {
				return nil, nil, err
			}
		}
		switch targetCommit.Op {
		case commitCreate:
			if targetExists {
				return nil, nil, ErrAlreadyExists
			}
		case commitUpdate, commitDelete:
			if !targetExists {
				return nil, nil, ErrNotFound
			}
			if parseStoredRV(targetCurrent.Metadata.ResourceVersion) != targetCommit.ExpectedRV {
				return nil, nil, ErrConflict
			}
		default:
			return nil, nil, ErrUnsupported
		}

		targetRV := currentRV + 1
		requestRV := currentRV + 2
		targetOut := cloneUnstructured(*targetCommit.Object)
		targetOut.Metadata.ResourceVersion = formatRV(targetRV)
		targetRaw, err := json.Marshal(targetOut)
		if err != nil {
			return nil, nil, err
		}
		targetEvent := newStoreEvent(targetCommit, targetRV, &targetOut)
		targetEventRaw, err := json.Marshal(targetEvent)
		if err != nil {
			return nil, nil, err
		}

		status.Phase = AdmissionCommittedPhase
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
		status.TargetResourceVersion = targetOut.Metadata.ResourceVersion
		status.TargetObject = targetObj
		requestUpdated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return nil, nil, err
		}
		requestOut := cloneUnstructured(*requestUpdated)
		requestOut.Metadata.ResourceVersion = formatRV(requestRV)
		requestOut.Metadata.UpdatedAt = now
		requestOutRaw, err := json.Marshal(requestOut)
		if err != nil {
			return nil, nil, err
		}
		requestEvent := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &requestOut,
			EventType:         WatchModified,
			Changed:           []string{"status"},
		}, requestRV, &requestOut)
		requestEventRaw, err := json.Marshal(requestEvent)
		if err != nil {
			return nil, nil, err
		}

		cmps := []clientv3.Cmp{
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(currentRaw)),
			clientv3.Compare(clientv3.Value(lockKey), "=", req.Name),
		}
		if targetCommit.Op == commitCreate {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(targetCommit.Ref)), "=", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(targetCommit.Ref)), "=", string(targetCurrentRaw)))
		}
		ops := s.eventStorageOps(targetCommit.Ref, targetRV, targetEventRaw)
		ops = append(ops, s.eventStorageOps(requestRef, requestRV, requestEventRaw)...)
		ops = append(ops, s.notifyOpsForRefs(requestRV, targetCommit.Ref, requestRef)...)
		ops = append(ops, clientv3.OpPut(s.metaKey("rv"), formatRV(requestRV)))
		if targetCommit.Op == commitDelete {
			ops = append(ops, clientv3.OpDelete(s.objectKey(targetCommit.Ref)))
		} else {
			ops = append(ops, clientv3.OpPut(s.objectKey(targetCommit.Ref), string(targetRaw)))
		}
		ops = append(ops,
			clientv3.OpPut(s.objectKey(requestRef), string(requestOutRaw)),
			clientv3.OpDelete(lockKey),
		)
		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&targetOut), cloneUnstructuredPtr(&requestOut), nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, nil, ErrConflict
}

func (s *etcdStore) rejectAdmission(ctx context.Context, req rejectAdmissionRequest) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	return s.finishAdmission(ctx, req.Name, func(spec AdmissionRequestSpec, status *AdmissionRequestStatus) {
		now := time.Now().UTC()
		status.Phase = AdmissionRejectedPhase
		status.RejectedRule = req.Decision.Rule
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
	})
}

func (s *etcdStore) expireAdmission(ctx context.Context, name string, phase AdmissionPhase, message string) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	return s.finishAdmission(ctx, name, func(spec AdmissionRequestSpec, status *AdmissionRequestStatus) {
		status.Phase = phase
		status.Message = message
		status.DecidedAt = time.Now().UTC()
	})
}

func (s *etcdStore) eventsAfter(ctx context.Context, after uint64, scope resourceScope, limit int) ([]resourceEvent, uint64, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, 0, err
	}
	compacted, err := s.readUint(ctx, s.metaKey("compacted-rv"))
	if err != nil {
		return nil, 0, err
	}
	current, err := s.currentRV(ctx)
	if err != nil {
		return nil, 0, err
	}
	if after < compacted {
		return nil, current, ErrResourceVersionTooOld
	}
	prefix := s.eventPrefix(scope)
	rangeEnd := clientv3.GetPrefixRangeEnd(prefix)
	if scope.Resource != "" && scope.Namespace == "" {
		rangeEnd = prefix + ":"
	}
	resp, err := s.client.Get(
		ctx,
		prefix+rvKey(after+1),
		clientv3.WithRange(rangeEnd),
		clientv3.WithLimit(int64(limit)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, 0, err
	}
	events := make([]resourceEvent, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var event resourceEvent
		if err := json.Unmarshal(kv.Value, &event); err != nil {
			return nil, 0, err
		}
		events = append(events, cloneEvent(event))
	}
	return events, current, nil
}

func (s *etcdStore) cleanupEvents(ctx context.Context) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	if err := s.cleanupAdmissions(ctx); err != nil {
		return err
	}
	current, err := s.currentRV(ctx)
	if err != nil {
		return err
	}
	if current <= uint64(s.retention) {
		return nil
	}
	before := current - uint64(s.retention)
	if err := s.advanceCompactedRV(ctx, before); err != nil {
		return err
	}
	return s.deleteEventsBefore(ctx, before)
}

func (s *etcdStore) subscribe(ctx context.Context, scope resourceScope) (<-chan struct{}, func(), error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, nil, err
	}
	key := s.notifyKey(scope)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	watchCtx, cancelWatch := context.WithCancel(ctx)
	out := make(chan struct{}, 1)
	go func() {
		defer close(out)
		revision := resp.Header.Revision + 1
		backoff := 50 * time.Millisecond
		for {
			if watchCtx.Err() != nil {
				return
			}
			watchCh := s.client.Watch(watchCtx, key, clientv3.WithRev(revision))
			restart := false
			for !restart {
				select {
				case <-watchCtx.Done():
					return
				case watchResp, ok := <-watchCh:
					if !ok {
						restart = true
						break
					}
					if watchResp.Header.Revision > 0 {
						revision = watchResp.Header.Revision + 1
					}
					if watchResp.Err() != nil {
						restart = true
						break
					}
					if len(watchResp.Events) > 0 {
						select {
						case out <- struct{}{}:
						default:
						}
					}
				}
			}
			timer := time.NewTimer(backoff)
			select {
			case <-watchCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < time.Second {
				backoff *= 2
			}
		}
	}()
	return out, cancelWatch, nil
}

func (s *etcdStore) acquireNode(ctx context.Context, name string, ttl time.Duration) (string, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return "", err
	}
	token, err := randomToken("node")
	if err != nil {
		return "", err
	}
	lease, err := s.client.Grant(ctx, etcdLeaseSeconds(ttl))
	if err != nil {
		return "", err
	}
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(s.nodeLeaseKey(name)), "=", 0)).
		Then(clientv3.OpPut(s.nodeLeaseKey(name), token, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return "", err
	}
	if !resp.Succeeded {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return "", ErrNodeAlreadyExists
	}
	return token, nil
}

func (s *etcdStore) renewNode(ctx context.Context, name, token string, ttl time.Duration) error {
	if err := s.ensureOpen(ctx); err != nil {
		return err
	}
	lease, err := s.client.Grant(ctx, etcdLeaseSeconds(ttl))
	if err != nil {
		return err
	}
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(s.nodeLeaseKey(name)), "=", token)).
		Then(clientv3.OpPut(s.nodeLeaseKey(name), token, clientv3.WithLease(lease.ID))).
		Commit()
	if err != nil {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return err
	}
	if !resp.Succeeded {
		_, _ = s.client.Revoke(context.Background(), lease.ID)
		return ErrNodeLeaseLost
	}
	return nil
}

func (s *etcdStore) releaseNode(ctx context.Context, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil
	}
	_, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(s.nodeLeaseKey(name)), "=", token)).
		Then(clientv3.OpDelete(s.nodeLeaseKey(name))).
		Commit()
	return err
}

func (s *etcdStore) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *etcdStore) ensureOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *etcdStore) currentRV(ctx context.Context) (uint64, error) {
	return s.readUint(ctx, s.metaKey("rv"))
}

func (s *etcdStore) readMetaRV(ctx context.Context) (uint64, string, int64, error) {
	resp, err := s.client.Get(ctx, s.metaKey("rv"), clientv3.WithLimit(1))
	if err != nil {
		return 0, "", 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, "", 0, nil
	}
	raw := string(resp.Kvs[0].Value)
	return parseStoredRV(raw), raw, resp.Kvs[0].Version, nil
}

func (s *etcdStore) readUint(ctx context.Context, key string) (uint64, error) {
	resp, err := s.client.Get(ctx, key, clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	return parseStoredRV(string(resp.Kvs[0].Value)), nil
}

func (s *etcdStore) metaRVCompare(raw string, version int64) clientv3.Cmp {
	if version == 0 {
		return clientv3.Compare(clientv3.Version(s.metaKey("rv")), "=", 0)
	}
	return clientv3.Compare(clientv3.Value(s.metaKey("rv")), "=", raw)
}

func (s *etcdStore) deleteEventsBefore(ctx context.Context, before uint64) error {
	prefix := s.eventPrefix(resourceScope{})
	end := prefix + rvKey(before+1)
	for {
		resp, err := s.client.Get(
			ctx,
			prefix,
			clientv3.WithRange(end),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			clientv3.WithLimit(60),
		)
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}
		ops := make([]clientv3.Op, 0, len(resp.Kvs)*3)
		for _, kv := range resp.Kvs {
			var event resourceEvent
			if err := json.Unmarshal(kv.Value, &event); err != nil {
				return err
			}
			rv := parseStoredRV(event.ResourceVersion)
			ops = append(ops,
				clientv3.OpDelete(s.eventAllKey(rv)),
				clientv3.OpDelete(s.eventResourceKey(event.Ref.Resource, rv)),
			)
			if event.Ref.Namespace != "" {
				ops = append(ops, clientv3.OpDelete(s.eventNamespaceKey(event.Ref, rv)))
			}
		}
		if _, err := s.client.Txn(ctx).Then(ops...).Commit(); err != nil {
			return err
		}
	}
}

func (s *etcdStore) advanceCompactedRV(ctx context.Context, before uint64) error {
	key := s.metaKey("compacted-rv")
	for {
		resp, err := s.client.Get(ctx, key, clientv3.WithLimit(1))
		if err != nil {
			return err
		}
		var current uint64
		var cmp clientv3.Cmp
		if len(resp.Kvs) == 0 {
			cmp = clientv3.Compare(clientv3.Version(key), "=", 0)
		} else {
			current = parseStoredRV(string(resp.Kvs[0].Value))
			if before <= current {
				return nil
			}
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)
		}
		txnResp, err := s.client.Txn(ctx).If(cmp).Then(clientv3.OpPut(key, formatRV(before))).Commit()
		if err != nil {
			return err
		}
		if txnResp.Succeeded {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *etcdStore) updateAdmissionRequestTxn(ctx context.Context, ref objectRef, currentRaw []byte, metaRaw string, metaVersion int64, updated *Unstructured, changed []string) (*Unstructured, error) {
	currentRV := parseStoredRV(updated.Metadata.ResourceVersion)
	_ = currentRV
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		out := cloneUnstructured(*updated)
		current, err := s.client.Get(ctx, s.objectKey(ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(current.Kvs) == 0 {
			return nil, ErrNotFound
		}
		if string(current.Kvs[0].Value) != string(currentRaw) {
			return nil, ErrConflict
		}
		rv, raw, version, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		if raw != metaRaw || version != metaVersion {
			metaRaw, metaVersion = raw, version
		}
		nextRV := rv + 1
		out.Metadata.ResourceVersion = formatRV(nextRV)
		out.Metadata.UpdatedAt = time.Now().UTC()
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               ref,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchModified,
			Changed:           changed,
		}, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		ops := s.eventWriteOps(ref, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(ref), string(objectRaw)),
		)
		txnResp, err := s.client.Txn(ctx).If(
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(ref)), "=", string(currentRaw)),
		).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) cleanupAdmissions(ctx context.Context) error {
	now := time.Now().UTC()
	resp, err := s.client.Get(ctx, s.objectPrefix(resourceScope{Resource: ResourceAdmissionRequests}), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return err
	}
	terminal := make([]Unstructured, 0)
	for _, kv := range resp.Kvs {
		var obj Unstructured
		if err := json.Unmarshal(kv.Value, &obj); err != nil {
			return err
		}
		spec, status, err := decodeAdmissionRequest(obj)
		if err != nil {
			return err
		}
		if status.Phase == AdmissionPendingPhase && !spec.ExpiresAt.IsZero() && !spec.ExpiresAt.After(now) {
			if _, err := s.expireAdmission(ctx, obj.Metadata.Name, AdmissionExpiredPhase, "admission timeout"); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				return err
			}
			status.Phase = AdmissionExpiredPhase
			status.Message = "admission timeout"
			status.DecidedAt = now
			updated, err := encodeAdmissionRequest(obj.Metadata, spec, status)
			if err != nil {
				return err
			}
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
		if err := s.deleteAdmissionRequest(ctx, obj.Metadata.Name); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return nil
}

func (s *etcdStore) finishAdmission(ctx context.Context, name string, mutate func(spec AdmissionRequestSpec, status *AdmissionRequestStatus)) (*Unstructured, error) {
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		_ = currentRV
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(requestResp.Kvs) == 0 {
			return nil, ErrNotFound
		}
		currentRaw := append([]byte(nil), requestResp.Kvs[0].Value...)
		var current Unstructured
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			return nil, err
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return nil, err
		}
		if status.Phase != AdmissionPendingPhase {
			return cloneUnstructuredPtr(&current), nil
		}
		lockKey := s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name})
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(lockResp.Kvs) == 0 || string(lockResp.Kvs[0].Value) != name {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		mutate(spec, &status)
		updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return nil, err
		}
		nextRV := parseStoredRV(metaRaw) + 1
		if metaVersion == 0 {
			nextRV = 1
		}
		out := cloneUnstructured(*updated)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		out.Metadata.UpdatedAt = time.Now().UTC()
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchModified,
			Changed:           []string{"status"},
		}, nextRV, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		ops := s.eventWriteOps(requestRef, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(requestRef), string(objectRaw)),
			clientv3.OpDelete(lockKey),
		)
		txnResp, err := s.client.Txn(ctx).If(
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(currentRaw)),
			clientv3.Compare(clientv3.Value(lockKey), "=", name),
		).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) deleteAdmissionRequest(ctx context.Context, name string) error {
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	resp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return ErrNotFound
	}
	txnResp, err := s.client.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(resp.Kvs[0].Value)),
	).Then(
		clientv3.OpDelete(s.objectKey(requestRef)),
	).Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return ErrConflict
	}
	return nil
}

func (s *etcdStore) eventWriteOps(ref objectRef, rv uint64, eventRaw []byte) []clientv3.Op {
	ops := s.eventStorageOps(ref, rv, eventRaw)
	ops = append(ops, s.notifyOpsForRefs(rv, ref)...)
	return ops
}

func (s *etcdStore) eventStorageOps(ref objectRef, rv uint64, eventRaw []byte) []clientv3.Op {
	ops := []clientv3.Op{
		clientv3.OpPut(s.eventAllKey(rv), string(eventRaw)),
		clientv3.OpPut(s.eventResourceKey(ref.Resource, rv), string(eventRaw)),
	}
	if ref.Namespace != "" {
		ops = append(ops, clientv3.OpPut(s.eventNamespaceKey(ref, rv), string(eventRaw)))
	}
	return ops
}

func (s *etcdStore) notifyOpsForRefs(rv uint64, refs ...objectRef) []clientv3.Op {
	keys := map[string]struct{}{
		s.notifyKey(resourceScope{}): {},
	}
	for _, ref := range refs {
		keys[s.notifyKey(resourceScope{Resource: ref.Resource, AllNamespaces: true})] = struct{}{}
		if ref.Namespace != "" {
			keys[s.notifyKey(resourceScope{Resource: ref.Resource, Namespace: ref.Namespace})] = struct{}{}
		}
	}
	ops := make([]clientv3.Op, 0, len(keys))
	for key := range keys {
		ops = append(ops, clientv3.OpPut(key, formatRV(rv)))
	}
	return ops
}

func (s *etcdStore) metaKey(name string) string {
	return s.prefix + "meta/" + name
}

func (s *etcdStore) admissionLockKey(ref objectRef) string {
	if ref.Namespace != "" {
		return s.prefix + "locks/admission/" + ref.Resource + "/namespaces/" + ref.Namespace + "/" + ref.Name
	}
	return s.prefix + "locks/admission/" + ref.Resource + "/" + ref.Name
}

func (s *etcdStore) objectKey(ref objectRef) string {
	if ref.Namespace != "" {
		return s.prefix + "objects/" + ref.Resource + "/namespaces/" + ref.Namespace + "/" + ref.Name
	}
	return s.prefix + "objects/" + ref.Resource + "/" + ref.Name
}

func (s *etcdStore) objectPrefix(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "objects/"
	}
	if scope.Namespace != "" {
		return s.prefix + "objects/" + scope.Resource + "/namespaces/" + scope.Namespace + "/"
	}
	return s.prefix + "objects/" + scope.Resource + "/"
}

func (s *etcdStore) eventPrefix(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "events/all/"
	}
	if scope.Namespace != "" {
		return s.prefix + "events/resources/" + scope.Resource + "/namespaces/" + scope.Namespace + "/"
	}
	return s.prefix + "events/resources/" + scope.Resource + "/"
}

func (s *etcdStore) eventAllKey(rv uint64) string {
	return s.eventPrefix(resourceScope{}) + rvKey(rv)
}

func (s *etcdStore) eventResourceKey(resource string, rv uint64) string {
	return s.eventPrefix(resourceScope{Resource: resource, AllNamespaces: true}) + rvKey(rv)
}

func (s *etcdStore) eventNamespaceKey(ref objectRef, rv uint64) string {
	return s.eventPrefix(resourceScope{Resource: ref.Resource, Namespace: ref.Namespace}) + rvKey(rv)
}

func (s *etcdStore) notifyKey(scope resourceScope) string {
	if scope.Resource == "" {
		return s.prefix + "notify/all"
	}
	if scope.Namespace != "" {
		return s.prefix + "notify/resources/" + scope.Resource + "/namespaces/" + scope.Namespace
	}
	return s.prefix + "notify/resources/" + scope.Resource
}

func (s *etcdStore) nodeLeaseKey(name string) string {
	return s.prefix + "leases/nodes/" + name
}

func etcdLeaseSeconds(ttl time.Duration) int64 {
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}
