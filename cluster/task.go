package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

const (
	defaultTaskMaxAttempts = 3
	defaultTaskListLimit   = 100
	defaultTaskPruneLimit  = 100
	maxTaskErrorLength     = 4096
)

// TaskID uniquely identifies a task within one cluster.
type TaskID string

// TaskStatus describes a task lifecycle state.
type TaskStatus string

const (
	// TaskPending is visible for claiming when NextVisibleAt has passed.
	TaskPending TaskStatus = "pending"
	// TaskRunning is currently claimed by a worker.
	TaskRunning TaskStatus = "running"
	// TaskRetrying is waiting for NextVisibleAt before another attempt.
	TaskRetrying TaskStatus = "retrying"
	// TaskSucceeded finished successfully.
	TaskSucceeded TaskStatus = "succeeded"
	// TaskFailed exhausted retries or failed with a non-retryable error.
	TaskFailed TaskStatus = "failed"
	// TaskCanceled was canceled before or during execution.
	TaskCanceled TaskStatus = "canceled"
	// TaskCancelRequested asks a running handler to finish as canceled.
	TaskCancelRequested TaskStatus = "cancel_requested"
)

// Task is the persisted task state.
//
// Tasks are executed at least once. Handlers must be idempotent and should use
// ID, DedupKey, or FencingToken when applying external side effects.
type Task struct {
	ID      TaskID `json:"id"`
	Topic   string `json:"topic"`
	Payload []byte `json:"payload,omitempty"`

	Metadata map[string]string `json:"metadata,omitempty"`

	Status TaskStatus `json:"status"`

	Attempt     int `json:"attempt"`
	MaxAttempts int `json:"max_attempts"`

	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	NextVisibleAt time.Time `json:"next_visible_at"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	FinishedAt    time.Time `json:"finished_at,omitempty"`

	CreatedByNodeID    string `json:"created_by_node_id"`
	CreatedBySessionID string `json:"created_by_session_id"`

	WorkerNodeID    string    `json:"worker_node_id,omitempty"`
	WorkerSessionID string    `json:"worker_session_id,omitempty"`
	ClaimToken      string    `json:"claim_token,omitempty"`
	ClaimExpiresAt  time.Time `json:"claim_expires_at,omitempty"`

	DedupKey     string `json:"dedup_key,omitempty"`
	FencingToken string `json:"fencing_token,omitempty"`

	CancelRequested bool   `json:"cancel_requested,omitempty"`
	LastError       string `json:"last_error,omitempty"`
}

// SubmitTask describes a task creation request.
//
// Topic is required and must not contain "/". If ID is empty, SubmitTask
// generates one. DedupKey suppresses duplicate active tasks for the same topic.
// LeaderOnly requires this node to be leader and captures the current fencing
// token into the task.
type SubmitTask struct {
	ID      TaskID
	Topic   string
	Payload []byte

	Metadata map[string]string

	Delay       time.Duration
	DedupKey    string
	MaxAttempts int

	LeaderOnly bool
}

// TaskClaim is the TTL-backed worker claim record stored separately from Task.
type TaskClaim struct {
	TaskID TaskID `json:"task_id"`

	WorkerNodeID    string `json:"worker_node_id"`
	WorkerSessionID string `json:"worker_session_id"`
	ClaimToken      string `json:"claim_token"`

	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// TaskDedup points a topic and deduplication key to the active or latest task.
type TaskDedup struct {
	Topic     string    `json:"topic"`
	DedupKey  string    `json:"dedup_key"`
	TaskID    TaskID    `json:"task_id"`
	CreatedAt time.Time `json:"created_at"`
}

// ListTaskOptions filters and paginates task listing.
type ListTaskOptions struct {
	Topic  string
	Status TaskStatus
	Cursor string
	Limit  int
}

// ListTaskResult contains one page of tasks.
type ListTaskResult struct {
	Tasks      []Task
	NextCursor string
	HasMore    bool
}

// RequeueOptions controls manual requeue behavior.
type RequeueOptions struct {
	Delay          time.Duration
	ResetAttempts  bool
	ClearLastError bool
}

// TaskStats contains aggregate task counters for a topic or the whole cluster.
type TaskStats struct {
	Pending         int64
	Running         int64
	Retrying        int64
	Succeeded       int64
	Failed          int64
	Canceled        int64
	CancelRequested int64
	Total           int64
}

// PruneTaskOptions controls deletion of old terminal tasks.
//
// Before is required. Statuses must be terminal states: succeeded, failed, or
// canceled. If Statuses is empty, all terminal states are eligible.
type PruneTaskOptions struct {
	Topic    string
	Statuses []TaskStatus
	Before   time.Time
	Limit    int
}

// TaskHandler processes one claimed task.
//
// Returning nil marks the task succeeded. Returning NonRetryable(err) fails it
// immediately. Returning ErrTaskCanceled marks it canceled.
type TaskHandler func(context.Context, Task) error

// WorkerOptions controls task polling, claiming, retries, and drain behavior.
type WorkerOptions struct {
	Concurrency  int
	AckTimeout   time.Duration
	PollInterval time.Duration

	RetryDelay    time.Duration
	MaxRetryDelay time.Duration
	BackoffFactor float64

	DrainTimeout time.Duration
}

type workerRegistration struct {
	topic   string
	handler TaskHandler
	opts    WorkerOptions
}

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string {
	return e.err.Error()
}

func (e nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryable wraps an error so the worker fails the task without retrying it.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

// IsNonRetryable reports whether err was wrapped with NonRetryable.
func IsNonRetryable(err error) bool {
	var target nonRetryableError
	return errors.As(err, &target)
}

// SubmitTask persists a new task and returns its ID.
//
// If req.DedupKey points to an active task for the same topic, SubmitTask returns
// that existing task ID. If LeaderOnly is true and this node is not leader,
// SubmitTask returns ErrNotLeader.
func (n *Node) SubmitTask(ctx context.Context, req SubmitTask) (TaskID, error) {
	if err := validateSubmitTask(req); err != nil {
		return "", err
	}
	if req.LeaderOnly && !n.IsLeader() {
		return "", ErrNotLeader
	}

	task, raw, err := n.newTask(req)
	if err != nil {
		return "", err
	}
	data := n.taskData()
	if req.DedupKey == "" {
		ok, err := data.PutIfNotExists(ctx, string(task.ID), raw, kv.TTLKeep)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", ErrInvalidTask
		}
		return task.ID, nil
	}

	dedupKey := taskDedupKey(req.Topic, req.DedupKey)
	dedup := TaskDedup{
		Topic:     req.Topic,
		DedupKey:  req.DedupKey,
		TaskID:    task.ID,
		CreatedAt: task.CreatedAt,
	}
	dedupRaw, err := marshalString(dedup)
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < 16; attempt++ {
		existingRaw, err := n.taskDedup().Get(ctx, dedupKey)
		if errors.Is(err, kv.ErrKeyNotFound) {
			ok, putErr := data.PutIfNotExists(ctx, string(task.ID), raw, kv.TTLKeep)
			if putErr != nil {
				return "", putErr
			}
			if !ok {
				return "", ErrInvalidTask
			}
			ok, putErr = n.taskDedup().PutIfNotExists(ctx, dedupKey, dedupRaw, kv.TTLKeep)
			if putErr != nil {
				_, _ = data.DeleteIfValue(context.Background(), string(task.ID), raw)
				return "", putErr
			}
			if ok {
				return task.ID, nil
			}
			_, _ = data.DeleteIfValue(context.Background(), string(task.ID), raw)
			continue
		}
		if err != nil {
			return "", err
		}

		var existingDedup TaskDedup
		if json.Unmarshal([]byte(existingRaw), &existingDedup) == nil {
			existingTask, taskErr := n.GetTask(ctx, existingDedup.TaskID)
			if taskErr == nil && !existingTask.finalized() {
				return existingDedup.TaskID, nil
			}
		}

		ok, err := data.PutIfNotExists(ctx, string(task.ID), raw, kv.TTLKeep)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", ErrInvalidTask
		}
		ok, err = n.taskDedup().CompareAndSwap(ctx, dedupKey, existingRaw, dedupRaw)
		if err != nil {
			_, _ = data.DeleteIfValue(context.Background(), string(task.ID), raw)
			return "", err
		}
		if ok {
			return task.ID, nil
		}
		_, _ = data.DeleteIfValue(context.Background(), string(task.ID), raw)
	}
	return "", ErrInvalidTask
}

func validateSubmitTask(req SubmitTask) error {
	if strings.TrimSpace(req.Topic) == "" || strings.Contains(req.Topic, "/") {
		return ErrInvalidTask
	}
	if req.ID != "" && invalidKey(string(req.ID)) {
		return ErrInvalidTask
	}
	if req.Delay < 0 {
		return ErrInvalidTask
	}
	return nil
}

func (n *Node) newTask(req SubmitTask) (Task, string, error) {
	id := req.ID
	if id == "" {
		generated, err := randomID("task", 18)
		if err != nil {
			return Task{}, "", err
		}
		id = TaskID(generated)
	}
	now := time.Now()
	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultTaskMaxAttempts
	}
	task := Task{
		ID:                 id,
		Topic:              req.Topic,
		Payload:            req.Payload,
		Metadata:           req.Metadata,
		Status:             TaskPending,
		MaxAttempts:        maxAttempts,
		CreatedAt:          now,
		UpdatedAt:          now,
		NextVisibleAt:      now.Add(req.Delay),
		CreatedByNodeID:    n.cfg.NodeID,
		CreatedBySessionID: n.sessionID,
		DedupKey:           req.DedupKey,
	}
	if req.LeaderOnly {
		n.mu.RLock()
		if n.leader != nil {
			task.FencingToken = n.leader.FencingToken
		}
		n.mu.RUnlock()
	}
	raw, err := marshalString(task)
	return task, raw, err
}

// GetTask returns one task by ID.
//
// Missing tasks return ErrTaskNotFound.
func (n *Node) GetTask(ctx context.Context, id TaskID) (*Task, error) {
	if invalidKey(string(id)) {
		return nil, ErrInvalidTask
	}
	raw, err := n.taskData().Get(ctx, string(id))
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	task, err := decodeTask(raw)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// ListTasks scans task data and returns a filtered page.
//
// This is intended for small control-plane task sets. Cursor should be treated
// as an opaque value returned by a previous ListTasks call.
func (n *Node) ListTasks(ctx context.Context, opts ListTaskOptions) (*ListTaskResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultTaskListLimit
	}
	if opts.Status != "" && !validTaskStatus(opts.Status) {
		return nil, ErrInvalidTask
	}

	cursor := opts.Cursor
	out := make([]Task, 0, limit+1)
	matchedKeys := make([]string, 0, limit+1)
	for len(out) <= limit {
		resp, err := n.taskData().ListCursor(ctx, &kv.ListOptions{
			Cursor: cursor,
			Limit:  int64(limit),
		})
		if err != nil {
			return nil, err
		}
		if len(resp.Pairs) == 0 {
			return &ListTaskResult{Tasks: out}, nil
		}
		for _, pair := range resp.Pairs {
			task, err := decodeTask(pair.Value)
			if err != nil {
				continue
			}
			if opts.Topic != "" && task.Topic != opts.Topic {
				continue
			}
			if opts.Status != "" && task.Status != opts.Status {
				continue
			}
			out = append(out, task)
			matchedKeys = append(matchedKeys, pair.Key)
			if len(out) > limit {
				return &ListTaskResult{
					Tasks:      out[:limit],
					NextCursor: matchedKeys[limit-1],
					HasMore:    true,
				}, nil
			}
		}
		if !resp.HasMore {
			break
		}
		cursor = resp.Cursor
		if cursor == "" {
			break
		}
	}
	return &ListTaskResult{Tasks: out}, nil
}

// CancelTask cancels a pending task or requests cancellation for a running task.
//
// A running handler receives the same context it was invoked with; cancellation
// is cooperative and finalized when the handler returns.
func (n *Node) CancelTask(ctx context.Context, id TaskID) error {
	return n.updateTask(ctx, id, func(task *Task, now time.Time) error {
		switch task.Status {
		case TaskPending, TaskRetrying:
			task.Status = TaskCanceled
			task.CancelRequested = true
			task.FinishedAt = now
			task.NextVisibleAt = time.Time{}
			task.clearClaim()
			return nil
		case TaskRunning:
			task.Status = TaskCancelRequested
			task.CancelRequested = true
			return nil
		case TaskCancelRequested:
			task.CancelRequested = true
			return nil
		case TaskSucceeded, TaskFailed, TaskCanceled:
			return ErrTaskFinalized
		default:
			return ErrInvalidTask
		}
	})
}

// RequeueTask moves a non-running task back to pending.
//
// Running and cancel-requested tasks cannot be requeued because a worker may
// still own the claim.
func (n *Node) RequeueTask(ctx context.Context, id TaskID, opts RequeueOptions) error {
	if opts.Delay < 0 {
		return ErrInvalidTask
	}
	return n.updateTask(ctx, id, func(task *Task, now time.Time) error {
		switch task.Status {
		case TaskPending, TaskRetrying, TaskFailed, TaskCanceled:
		case TaskRunning, TaskCancelRequested:
			return ErrTaskClaimLost
		case TaskSucceeded:
			return ErrTaskFinalized
		default:
			return ErrInvalidTask
		}
		task.Status = TaskPending
		task.NextVisibleAt = now.Add(opts.Delay)
		task.FinishedAt = time.Time{}
		task.StartedAt = time.Time{}
		task.CancelRequested = false
		task.clearClaim()
		if opts.ResetAttempts {
			task.Attempt = 0
		}
		if opts.ClearLastError {
			task.LastError = ""
		}
		return nil
	})
}

// TaskStats scans tasks and returns aggregate counters.
//
// If topic is empty, all topics are included.
func (n *Node) TaskStats(ctx context.Context, topic string) (*TaskStats, error) {
	pairs, err := n.taskData().List(ctx, "")
	if err != nil {
		return nil, err
	}
	stats := &TaskStats{}
	for _, pair := range pairs {
		task, err := decodeTask(pair.Value)
		if err != nil || topic != "" && task.Topic != topic {
			continue
		}
		stats.Total++
		switch task.Status {
		case TaskPending:
			stats.Pending++
		case TaskRunning:
			stats.Running++
		case TaskRetrying:
			stats.Retrying++
		case TaskSucceeded:
			stats.Succeeded++
		case TaskFailed:
			stats.Failed++
		case TaskCanceled:
			stats.Canceled++
		case TaskCancelRequested:
			stats.CancelRequested++
		}
	}
	return stats, nil
}

// PruneTasks deletes old terminal tasks and their matching dedup records.
//
// PruneTasks requires opts.Before so callers do not accidentally delete fresh
// task history.
func (n *Node) PruneTasks(ctx context.Context, opts PruneTaskOptions) (int, error) {
	if opts.Before.IsZero() {
		return 0, ErrInvalidTask
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultTaskPruneLimit
	}
	statuses := make(map[TaskStatus]struct{})
	if len(opts.Statuses) == 0 {
		statuses[TaskSucceeded] = struct{}{}
		statuses[TaskFailed] = struct{}{}
		statuses[TaskCanceled] = struct{}{}
	} else {
		for _, status := range opts.Statuses {
			if !terminalTaskStatus(status) {
				return 0, ErrInvalidTask
			}
			statuses[status] = struct{}{}
		}
	}

	pairs, err := n.taskData().List(ctx, "")
	if err != nil {
		return 0, err
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})

	pruned := 0
	for _, pair := range pairs {
		if pruned >= limit {
			break
		}
		task, err := decodeTask(pair.Value)
		if err != nil || opts.Topic != "" && task.Topic != opts.Topic {
			continue
		}
		if _, ok := statuses[task.Status]; !ok || task.FinishedAt.IsZero() || !task.FinishedAt.Before(opts.Before) {
			continue
		}
		deleted, err := n.taskData().DeleteIfValue(ctx, string(task.ID), pair.Value)
		if err != nil {
			return pruned, err
		}
		if !deleted {
			continue
		}
		if task.DedupKey != "" {
			n.deleteTaskDedup(ctx, task)
		}
		pruned++
	}
	return pruned, nil
}

// Handle registers a worker handler for a topic.
//
// Handlers may be registered before or after Run. Each claimed task is processed
// at least once, so handlers must be idempotent.
func (n *Node) Handle(topic string, handler TaskHandler, opts WorkerOptions) error {
	if strings.TrimSpace(topic) == "" || strings.Contains(topic, "/") || handler == nil {
		return ErrInvalidTask
	}
	opts = normalizeWorkerOptions(opts, n.cfg.DrainTimeout)
	registration := workerRegistration{topic: topic, handler: handler, opts: opts}

	n.workerMu.Lock()
	defer n.workerMu.Unlock()
	n.workers = append(n.workers, registration)
	if n.workerCtx != nil {
		n.startWorkerRegistrationLocked(n.workerCtx, registration)
	}
	return nil
}

func normalizeWorkerOptions(opts WorkerOptions, drainTimeout time.Duration) WorkerOptions {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.AckTimeout <= 0 {
		opts.AckTimeout = 30 * time.Second
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = time.Second
	}
	if opts.MaxRetryDelay <= 0 {
		opts.MaxRetryDelay = 30 * time.Second
	}
	if opts.BackoffFactor <= 0 {
		opts.BackoffFactor = 1
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = drainTimeout
	}
	return opts
}

func (n *Node) startWorkers(ctx context.Context) {
	n.workerMu.Lock()
	defer n.workerMu.Unlock()
	if n.workerCancel != nil {
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	n.workerCtx = workerCtx
	n.workerCancel = cancel
	for _, registration := range n.workers {
		n.startWorkerRegistrationLocked(workerCtx, registration)
	}
}

func (n *Node) startWorkerRegistrationLocked(ctx context.Context, registration workerRegistration) {
	for i := 0; i < registration.opts.Concurrency; i++ {
		n.workerLoopWG.Add(1)
		go func() {
			defer n.workerLoopWG.Done()
			n.workerLoop(ctx, registration)
		}()
	}
}

func (n *Node) stopWorkers(ctx context.Context) {
	n.workerMu.Lock()
	cancel := n.workerCancel
	workers := append([]workerRegistration(nil), n.workers...)
	n.workerCtx = nil
	n.workerCancel = nil
	n.workerMu.Unlock()
	if cancel != nil {
		cancel()
	}

	timeout := n.cfg.DrainTimeout
	for _, worker := range workers {
		if worker.opts.DrainTimeout > timeout {
			timeout = worker.opts.DrainTimeout
		}
	}
	waitCtx := ctx
	cancelWait := func() {}
	if timeout > 0 {
		waitCtx, cancelWait = context.WithTimeout(ctx, timeout)
	}
	defer cancelWait()

	done := make(chan struct{})
	go func() {
		n.workerLoopWG.Wait()
		n.workerActiveWG.Wait()
		close(done)
	}()
	select {
	case <-waitCtx.Done():
	case <-done:
	}
}

func (n *Node) workerLoop(ctx context.Context, registration workerRegistration) {
	for {
		if ctx.Err() != nil {
			return
		}
		if n.Phase() == PhaseDraining || n.Phase() == PhaseLeaving || n.Phase() == PhaseDead {
			if err := n.wait(ctx, registration.opts.PollInterval); err != nil {
				return
			}
			continue
		}

		task, claimRaw, ok, err := n.claimTask(ctx, registration)
		if err != nil || !ok {
			if waitErr := n.wait(ctx, registration.opts.PollInterval); waitErr != nil {
				return
			}
			continue
		}

		n.workerActiveWG.Add(1)
		handlerCtx, cancel := context.WithTimeout(ctx, registration.opts.AckTimeout)
		handlerErr := registration.handler(handlerCtx, task)
		cancel()
		_ = n.finishTask(context.Background(), task, claimRaw, handlerErr, registration.opts)
		n.workerActiveWG.Done()
	}
}

func (n *Node) claimTask(ctx context.Context, registration workerRegistration) (Task, string, bool, error) {
	pairs, err := n.taskData().List(ctx, "")
	if err != nil {
		return Task{}, "", false, err
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Key < pairs[j].Key
	})

	now := time.Now()
	for _, pair := range pairs {
		task, err := decodeTask(pair.Value)
		if err != nil || task.Topic != registration.topic {
			continue
		}
		if task.Status == TaskRunning || task.Status == TaskCancelRequested {
			_, _ = n.recoverExpiredTask(ctx, task, pair.Value, registration.opts, now)
			continue
		}
		if !task.claimable(now) {
			continue
		}

		claimToken, err := randomID("claim", 18)
		if err != nil {
			return Task{}, "", false, err
		}
		claim := TaskClaim{
			TaskID:          task.ID,
			WorkerNodeID:    n.cfg.NodeID,
			WorkerSessionID: n.sessionID,
			ClaimToken:      claimToken,
			AcquiredAt:      now,
			ExpiresAt:       now.Add(registration.opts.AckTimeout),
		}
		claimRaw, err := marshalString(claim)
		if err != nil {
			return Task{}, "", false, err
		}
		ok, err := n.taskClaims().PutIfNotExists(ctx, string(task.ID), claimRaw, registration.opts.AckTimeout)
		if err != nil || !ok {
			return Task{}, "", false, err
		}

		claimed := task
		claimed.Status = TaskRunning
		claimed.Attempt++
		claimed.WorkerNodeID = n.cfg.NodeID
		claimed.WorkerSessionID = n.sessionID
		claimed.ClaimToken = claimToken
		claimed.ClaimExpiresAt = claim.ExpiresAt
		claimed.StartedAt = now
		claimed.UpdatedAt = now
		updatedRaw, err := marshalString(claimed)
		if err != nil {
			_, _ = n.taskClaims().DeleteIfValue(context.Background(), string(task.ID), claimRaw)
			return Task{}, "", false, err
		}
		swapped, err := n.taskData().CompareAndSwap(ctx, string(task.ID), pair.Value, updatedRaw)
		if err != nil || !swapped {
			_, _ = n.taskClaims().DeleteIfValue(context.Background(), string(task.ID), claimRaw)
			return Task{}, "", false, err
		}
		return claimed, claimRaw, true, nil
	}
	return Task{}, "", false, nil
}

func (n *Node) recoverExpiredTask(ctx context.Context, task Task, raw string, opts WorkerOptions, now time.Time) (bool, error) {
	if _, err := n.taskClaims().Get(ctx, string(task.ID)); err == nil {
		return false, nil
	} else if err != nil && !errors.Is(err, kv.ErrKeyNotFound) {
		return false, err
	}

	if task.Status == TaskCancelRequested || task.CancelRequested {
		task.Status = TaskCanceled
		task.FinishedAt = now
		task.NextVisibleAt = time.Time{}
		task.LastError = ""
	} else if task.MaxAttempts > 0 && task.Attempt >= task.MaxAttempts {
		task.Status = TaskFailed
		task.FinishedAt = now
		task.NextVisibleAt = time.Time{}
		task.LastError = "claim expired"
	} else {
		task.Status = TaskRetrying
		task.NextVisibleAt = now.Add(retryDelayFor(opts, task.Attempt))
		task.LastError = "claim expired"
	}
	task.UpdatedAt = now
	task.clearClaim()
	updatedRaw, err := marshalString(task)
	if err != nil {
		return false, err
	}
	return n.taskData().CompareAndSwap(ctx, string(task.ID), raw, updatedRaw)
}

func (n *Node) finishTask(ctx context.Context, task Task, claimRaw string, handlerErr error, opts WorkerOptions) error {
	for attempt := 0; attempt < 8; attempt++ {
		raw, err := n.taskData().Get(ctx, string(task.ID))
		if err != nil {
			_, _ = n.taskClaims().DeleteIfValue(ctx, string(task.ID), claimRaw)
			if errors.Is(err, kv.ErrKeyNotFound) {
				return ErrTaskNotFound
			}
			return err
		}
		current, err := decodeTask(raw)
		if err != nil {
			return err
		}
		if current.ClaimToken != task.ClaimToken || current.WorkerSessionID != n.sessionID {
			return ErrTaskClaimLost
		}

		now := time.Now()
		current.UpdatedAt = now
		current.clearClaim()
		if current.CancelRequested || current.Status == TaskCancelRequested || errors.Is(handlerErr, ErrTaskCanceled) {
			current.Status = TaskCanceled
			current.FinishedAt = now
			current.NextVisibleAt = time.Time{}
			current.LastError = errorString(handlerErr)
		} else if handlerErr == nil {
			current.Status = TaskSucceeded
			current.FinishedAt = now
			current.NextVisibleAt = time.Time{}
			current.LastError = ""
		} else if IsNonRetryable(handlerErr) || current.MaxAttempts > 0 && current.Attempt >= current.MaxAttempts {
			current.Status = TaskFailed
			current.FinishedAt = now
			current.NextVisibleAt = time.Time{}
			current.LastError = errorString(handlerErr)
		} else {
			current.Status = TaskRetrying
			current.NextVisibleAt = now.Add(retryDelayFor(opts, current.Attempt))
			current.LastError = errorString(handlerErr)
		}

		updatedRaw, err := marshalString(current)
		if err != nil {
			return err
		}
		ok, err := n.taskData().CompareAndSwap(ctx, string(task.ID), raw, updatedRaw)
		if err != nil {
			return err
		}
		if ok {
			_, _ = n.taskClaims().DeleteIfValue(ctx, string(task.ID), claimRaw)
			return nil
		}
	}
	return ErrTaskClaimLost
}

func (n *Node) updateTask(ctx context.Context, id TaskID, mutate func(*Task, time.Time) error) error {
	if invalidKey(string(id)) {
		return ErrInvalidTask
	}
	for attempt := 0; attempt < 8; attempt++ {
		raw, err := n.taskData().Get(ctx, string(id))
		if err != nil {
			if errors.Is(err, kv.ErrKeyNotFound) {
				return ErrTaskNotFound
			}
			return err
		}
		task, err := decodeTask(raw)
		if err != nil {
			return err
		}
		now := time.Now()
		if err := mutate(&task, now); err != nil {
			return err
		}
		task.UpdatedAt = now
		updatedRaw, err := marshalString(task)
		if err != nil {
			return err
		}
		ok, err := n.taskData().CompareAndSwap(ctx, string(id), raw, updatedRaw)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return ErrTaskClaimLost
}

func (n *Node) deleteTaskDedup(ctx context.Context, task Task) {
	raw, err := n.taskDedup().Get(ctx, taskDedupKey(task.Topic, task.DedupKey))
	if err != nil {
		return
	}
	var dedup TaskDedup
	if json.Unmarshal([]byte(raw), &dedup) == nil && dedup.TaskID == task.ID {
		_, _ = n.taskDedup().DeleteIfValue(ctx, taskDedupKey(task.Topic, task.DedupKey), raw)
	}
}

func (n *Node) taskData() kv.KV {
	return n.root.Child("tasks", "data")
}

func (n *Node) taskClaims() kv.KV {
	return n.root.Child("tasks", "claims")
}

func (n *Node) taskDedup() kv.KV {
	return n.root.Child("tasks", "dedup")
}

func decodeTask(raw string) (Task, error) {
	var task Task
	if err := json.Unmarshal([]byte(raw), &task); err != nil {
		return Task{}, err
	}
	return task, nil
}

func (t Task) finalized() bool {
	return terminalTaskStatus(t.Status)
}

func (t Task) claimable(now time.Time) bool {
	return (t.Status == TaskPending || t.Status == TaskRetrying) && !now.Before(t.NextVisibleAt)
}

func (t *Task) clearClaim() {
	t.WorkerNodeID = ""
	t.WorkerSessionID = ""
	t.ClaimToken = ""
	t.ClaimExpiresAt = time.Time{}
}

func terminalTaskStatus(status TaskStatus) bool {
	return status == TaskSucceeded || status == TaskFailed || status == TaskCanceled
}

func validTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskPending, TaskRunning, TaskRetrying, TaskSucceeded, TaskFailed, TaskCanceled, TaskCancelRequested:
		return true
	default:
		return false
	}
}

func taskDedupKey(topic, dedupKey string) string {
	sum := sha256.Sum256([]byte(topic + "\x00" + dedupKey))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func retryDelayFor(opts WorkerOptions, attempt int) time.Duration {
	if attempt <= 1 || opts.BackoffFactor == 1 {
		return opts.RetryDelay
	}
	delay := float64(opts.RetryDelay)
	delay *= math.Pow(opts.BackoffFactor, float64(attempt-1))
	if delay > float64(opts.MaxRetryDelay) {
		return opts.MaxRetryDelay
	}
	return time.Duration(delay)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > maxTaskErrorLength {
		return msg[:maxTaskErrorLength]
	}
	return msg
}

func (s TaskStatus) String() string {
	return string(s)
}

func (id TaskID) String() string {
	return string(id)
}

func (s TaskStatus) MarshalText() ([]byte, error) {
	return []byte(s), nil
}

func (id TaskID) MarshalText() ([]byte, error) {
	return []byte(id), nil
}

func (s *TaskStatus) UnmarshalText(data []byte) error {
	status := TaskStatus(data)
	if status != "" && !validTaskStatus(status) {
		return fmt.Errorf("%w: invalid task status", ErrInvalidTask)
	}
	*s = status
	return nil
}

func (id *TaskID) UnmarshalText(data []byte) error {
	*id = TaskID(data)
	return nil
}
