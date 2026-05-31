package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	defaultMasterName = "default"

	masterTransitionAcquired = "acquired"
	masterTransitionExpired  = "expired"
	masterTransitionReleased = "released"
	masterTransitionLost     = "lost"
)

func (c *Cluster) Masters() *Resource[MasterSpec, MasterStatus] {
	return c.masters
}

func (c *Cluster) Master(ctx context.Context) (*MasterInfo, error) {
	if err := c.ensureActive(ctx); err != nil {
		return nil, err
	}
	master, err := c.ensureMaster(ctx)
	if err != nil {
		return nil, err
	}
	return masterInfo(master, time.Now().UTC()), nil
}

func (c *Cluster) IsMaster(ctx context.Context) (bool, error) {
	master, err := c.Master(ctx)
	if err != nil {
		return false, err
	}
	return master.Valid && master.Node == c.options.NodeName, nil
}

func (c *Cluster) WatchMaster(ctx context.Context, opts WatchOptions) (<-chan MasterWatchEvent, error) {
	opts.Name = defaultMasterName
	events, err := c.masters.WatchStatus(ctx, opts)
	if err != nil {
		return nil, err
	}
	out := make(chan MasterWatchEvent, c.options.WatchBufferSize)
	go c.forwardMasterEvents(ctx, events, out)
	return out, nil
}

func (c *Cluster) MasterHistory(ctx context.Context, limit int) ([]MasterTransition, error) {
	if err := c.ensureActive(ctx); err != nil {
		return nil, err
	}
	master, err := c.ensureMaster(ctx)
	if err != nil {
		return nil, err
	}
	history := append([]MasterTransition(nil), master.Status.History...)
	if limit > 0 && len(history) > limit {
		history = history[len(history)-limit:]
	}
	return history, nil
}

func (c *Cluster) StepDown(ctx context.Context) error {
	if err := c.ensureActive(ctx); err != nil {
		return err
	}
	return c.stepDownMaster(ctx, masterTransitionReleased, true)
}

func (c *Cluster) startMasterElection() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.masterCancel = cancel
	c.masterDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(c.options.MasterRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				timeout := c.options.MasterRenewInterval
				if timeout < time.Second {
					timeout = time.Second
				}
				if timeout > c.options.MasterLeaseTTL {
					timeout = c.options.MasterLeaseTTL
				}
				maintainCtx, maintainCancel := context.WithTimeout(context.Background(), timeout)
				err := c.maintainMaster(maintainCtx)
				maintainCancel()
				if errors.Is(err, ErrClosed) || errors.Is(err, ErrNodeLeaseLost) {
					return
				}
			}
		}
	}()
}

func (c *Cluster) ensureMaster(ctx context.Context) (*Object[MasterSpec, MasterStatus], error) {
	master, err := c.masters.Get(ctx, defaultMasterName)
	if err == nil {
		return master, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	created, err := c.masters.Create(ctx, defaultMasterName, MasterSpec{}, CreateOptions{})
	if err == nil {
		return created, nil
	}
	if !errors.Is(err, ErrAlreadyExists) {
		return nil, err
	}
	return c.masters.Get(ctx, defaultMasterName)
}

func (c *Cluster) maintainMaster(ctx context.Context) error {
	if err := c.ensureActive(ctx); err != nil {
		return err
	}
	master, err := c.ensureMaster(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	status := master.Status
	active := status.Node != "" && status.LeaseUntil.After(now)
	if status.Node == c.options.NodeName && active {
		status.LeaseUntil = now.Add(c.options.MasterLeaseTTL)
		status.RenewedAt = now
		_, err := c.masters.UpdateStatus(ctx, defaultMasterName, status, UpdateOptions{
			ResourceVersion: master.Metadata.ResourceVersion,
		})
		if errors.Is(err, ErrConflict) {
			return nil
		}
		return err
	}
	if active {
		return nil
	}

	reason := masterTransitionExpired
	if status.Node == "" {
		reason = masterTransitionAcquired
	}
	previousNode := status.Node
	previousLeaseUntil := status.LeaseUntil
	status.Term++
	status.Node = c.options.NodeName
	status.LeaseUntil = now.Add(c.options.MasterLeaseTTL)
	status.AcquiredAt = now
	status.RenewedAt = now
	status.History = c.appendMasterTransition(status.History, MasterTransition{
		Term:               status.Term,
		From:               previousNode,
		To:                 c.options.NodeName,
		Reason:             reason,
		At:                 now,
		ObservedBy:         c.options.NodeName,
		PreviousLeaseUntil: previousLeaseUntil,
	})
	_, err = c.masters.UpdateStatus(ctx, defaultMasterName, status, UpdateOptions{
		ResourceVersion: master.Metadata.ResourceVersion,
	})
	if errors.Is(err, ErrConflict) {
		return nil
	}
	return err
}

func (c *Cluster) stepDownMaster(ctx context.Context, reason string, requireMaster bool) error {
	ref := objectRef{Resource: ResourceMasters, Name: defaultMasterName}
	for attempt := 0; attempt < maxMutationRetries; attempt++ {
		oldObj, err := c.store.get(ctx, ref)
		if err != nil {
			if errors.Is(err, ErrNotFound) && !requireMaster {
				return nil
			}
			if errors.Is(err, ErrNotFound) {
				return ErrNotMaster
			}
			return err
		}
		oldStatus, err := masterStatusFromRaw(oldObj.Status)
		if err != nil {
			return err
		}
		if oldStatus.Node != c.options.NodeName || !oldStatus.LeaseUntil.After(time.Now().UTC()) {
			if requireMaster {
				return ErrNotMaster
			}
			return nil
		}
		_, err = c.masters.raw.mutateStatus(ctx, ref, parseStoredRV(oldObj.Metadata.ResourceVersion), nil, func(obj Unstructured) (Unstructured, error) {
			status, err := masterStatusFromRaw(obj.Status)
			if err != nil {
				return Unstructured{}, err
			}
			now := time.Now().UTC()
			if status.Node != c.options.NodeName || !status.LeaseUntil.After(now) {
				return Unstructured{}, ErrConflict
			}
			previousNode := status.Node
			previousLeaseUntil := status.LeaseUntil
			status.Term++
			status.Node = ""
			status.LeaseUntil = time.Time{}
			status.AcquiredAt = time.Time{}
			status.RenewedAt = now
			status.History = c.appendMasterTransition(status.History, MasterTransition{
				Term:               status.Term,
				From:               previousNode,
				Reason:             reason,
				At:                 now,
				ObservedBy:         c.options.NodeName,
				PreviousLeaseUntil: previousLeaseUntil,
			})
			return setMasterStatus(obj, status)
		})
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}

func (c *Cluster) forwardMasterEvents(
	ctx context.Context,
	events <-chan WatchEvent[MasterSpec, MasterStatus],
	out chan<- MasterWatchEvent,
) {
	defer close(out)
	for event := range events {
		if event.Error != nil || event.Type == WatchError {
			if !sendMasterWatchEvent(ctx, out, MasterWatchEvent{
				Type:            WatchError,
				ResourceVersion: event.ResourceVersion,
				Error:           event.Error,
			}) {
				return
			}
			continue
		}
		changedMaster := event.Type == WatchAdded || event.Type == WatchDeleted
		for _, path := range event.Changed {
			if path == "status.node" || path == "status.term" {
				changedMaster = true
				break
			}
		}
		if !changedMaster {
			continue
		}
		outEvent := MasterWatchEvent{
			Type:            event.Type,
			ResourceVersion: event.ResourceVersion,
		}
		if event.Object != nil {
			info := masterInfo(event.Object, time.Now().UTC())
			outEvent.Master = info
			outEvent.Transition = latestMasterTransition(event.Object.Status.History, info.Term)
		}
		if !sendMasterWatchEvent(ctx, out, outEvent) {
			return
		}
	}
}

func masterInfo(obj *Object[MasterSpec, MasterStatus], now time.Time) *MasterInfo {
	return &MasterInfo{
		Node:            obj.Status.Node,
		Term:            obj.Status.Term,
		LeaseUntil:      obj.Status.LeaseUntil,
		AcquiredAt:      obj.Status.AcquiredAt,
		RenewedAt:       obj.Status.RenewedAt,
		ResourceVersion: obj.Metadata.ResourceVersion,
		Valid:           obj.Status.Node != "" && obj.Status.LeaseUntil.After(now),
	}
}

func latestMasterTransition(history []MasterTransition, term uint64) *MasterTransition {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Term == term {
			transition := history[i]
			return &transition
		}
	}
	return nil
}

func masterStatusFromRaw(raw json.RawMessage) (MasterStatus, error) {
	var status MasterStatus
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &status); err != nil {
			return MasterStatus{}, err
		}
	}
	return status, nil
}

func setMasterStatus(obj Unstructured, status MasterStatus) (Unstructured, error) {
	raw, err := marshalValue(status)
	if err != nil {
		return Unstructured{}, err
	}
	obj.Status = raw
	return obj, nil
}

func (c *Cluster) appendMasterTransition(history []MasterTransition, transition MasterTransition) []MasterTransition {
	history = append(append([]MasterTransition(nil), history...), transition)
	if len(history) > c.options.MasterHistoryLimit {
		history = history[len(history)-c.options.MasterHistoryLimit:]
	}
	return history
}

func sendMasterWatchEvent(ctx context.Context, out chan<- MasterWatchEvent, event MasterWatchEvent) bool {
	select {
	case out <- event:
		return true
	case <-ctx.Done():
		return false
	}
}
