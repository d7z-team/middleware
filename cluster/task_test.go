package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/kv"
)

func newTaskNode(t *testing.T, clusterID string) *Node {
	t.Helper()
	store, err := kv.NewMemory("")
	require.NoError(t, err)
	n, err := New(testConfig(store, clusterID, "n1", nil))
	require.NoError(t, err)
	return n
}

func runTaskNode(t *testing.T, n *Node) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- n.Run(ctx)
		close(errCh)
	}()
	t.Cleanup(func() {
		cancel()
		_ = n.Close()
		select {
		case <-errCh:
		case <-time.After(time.Second):
			t.Fatal("cluster member did not stop")
		}
	})
}

func TestSubmitTaskDefaultsDelayAndGet(t *testing.T) {
	n := newTaskNode(t, "submit-default")
	id, err := n.SubmitTask(context.Background(), SubmitTask{
		Topic:   "jobs",
		Payload: []byte("payload"),
		Delay:   50 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	task, err := n.GetTask(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, TaskPending, task.Status)
	require.Equal(t, 0, task.Attempt)
	require.Equal(t, defaultTaskMaxAttempts, task.MaxAttempts)
	require.Equal(t, []byte("payload"), task.Payload)
	require.True(t, task.NextVisibleAt.After(task.CreatedAt))
}

func TestSubmitTaskLeaderOnlyAndValidation(t *testing.T) {
	n := newTaskNode(t, "leader-submit")
	_, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", LeaderOnly: true})
	require.ErrorIs(t, err, ErrNotLeader)

	_, err = n.SubmitTask(context.Background(), SubmitTask{Topic: ""})
	require.ErrorIs(t, err, ErrInvalidTask)
	_, err = n.SubmitTask(context.Background(), SubmitTask{Topic: "bad/topic"})
	require.ErrorIs(t, err, ErrInvalidTask)
	_, err = n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", Delay: -time.Second})
	require.ErrorIs(t, err, ErrInvalidTask)

	runTaskNode(t, n)
	waitLeader(t, n)
	id, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", LeaderOnly: true})
	require.NoError(t, err)
	task, err := n.GetTask(context.Background(), id)
	require.NoError(t, err)
	require.NotEmpty(t, task.FencingToken)
}

func TestSubmitTaskDedupReturnsActiveTaskAndReplacesFinalizedTask(t *testing.T) {
	n := newTaskNode(t, "dedup")
	first, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", DedupKey: "same"})
	require.NoError(t, err)
	second, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", DedupKey: "same"})
	require.NoError(t, err)
	require.Equal(t, first, second)

	require.NoError(t, n.updateTask(context.Background(), first, func(task *Task, now time.Time) error {
		task.Status = TaskSucceeded
		task.FinishedAt = now
		return nil
	}))
	third, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", DedupKey: "same"})
	require.NoError(t, err)
	require.NotEqual(t, first, third)
}

func TestListTasksFiltersAcrossPages(t *testing.T) {
	n := newTaskNode(t, "list")
	for i := 0; i < 5; i++ {
		_, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "other"})
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		_, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
		require.NoError(t, err)
	}

	result, err := n.ListTasks(context.Background(), ListTaskOptions{Topic: "jobs", Limit: 2})
	require.NoError(t, err)
	require.Len(t, result.Tasks, 2)
	require.True(t, result.HasMore)

	next, err := n.ListTasks(context.Background(), ListTaskOptions{Topic: "jobs", Cursor: result.NextCursor, Limit: 2})
	require.NoError(t, err)
	require.Len(t, next.Tasks, 1)
	require.False(t, next.HasMore)
}

func TestListTasksExactLimitHasNoMore(t *testing.T) {
	n := newTaskNode(t, "list-exact")
	for i := 0; i < 2; i++ {
		_, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
		require.NoError(t, err)
	}

	result, err := n.ListTasks(context.Background(), ListTaskOptions{Topic: "jobs", Limit: 2})
	require.NoError(t, err)
	require.Len(t, result.Tasks, 2)
	require.False(t, result.HasMore)
	require.Empty(t, result.NextCursor)
}

func TestCancelTaskPendingAndRunning(t *testing.T) {
	n := newTaskNode(t, "cancel")
	pendingID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
	require.NoError(t, err)
	require.NoError(t, n.CancelTask(context.Background(), pendingID))
	pending, err := n.GetTask(context.Background(), pendingID)
	require.NoError(t, err)
	require.Equal(t, TaskCanceled, pending.Status)

	block := make(chan struct{})
	started := make(chan Task, 1)
	require.NoError(t, n.Handle("jobs", func(ctx context.Context, task Task) error {
		started <- task
		<-block
		return nil
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, AckTimeout: time.Second}))
	runTaskNode(t, n)

	runningID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
	require.NoError(t, err)
	<-started
	require.NoError(t, n.CancelTask(context.Background(), runningID))
	close(block)

	require.Eventually(t, func() bool {
		task, err := n.GetTask(context.Background(), runningID)
		return err == nil && task.Status == TaskCanceled
	}, time.Second, 20*time.Millisecond)
}

func TestRequeueTaskRules(t *testing.T) {
	n := newTaskNode(t, "requeue")
	id, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
	require.NoError(t, err)
	require.NoError(t, n.updateTask(context.Background(), id, func(task *Task, now time.Time) error {
		task.Status = TaskFailed
		task.Attempt = 3
		task.LastError = "failed"
		task.FinishedAt = now
		return nil
	}))

	require.NoError(t, n.RequeueTask(context.Background(), id, RequeueOptions{
		Delay:          20 * time.Millisecond,
		ResetAttempts:  true,
		ClearLastError: true,
	}))
	task, err := n.GetTask(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, TaskPending, task.Status)
	require.Equal(t, 0, task.Attempt)
	require.Empty(t, task.LastError)
	require.True(t, task.NextVisibleAt.After(task.UpdatedAt))

	require.NoError(t, n.updateTask(context.Background(), id, func(task *Task, now time.Time) error {
		task.Status = TaskSucceeded
		task.FinishedAt = now
		return nil
	}))
	require.ErrorIs(t, n.RequeueTask(context.Background(), id, RequeueOptions{}), ErrTaskFinalized)
}

func TestWorkerRetrySuccessFailureAndNonRetryable(t *testing.T) {
	n := newTaskNode(t, "worker")
	var retryAttempts atomic.Int32
	var failAttempts atomic.Int32
	require.NoError(t, n.Handle("retry", func(ctx context.Context, task Task) error {
		if retryAttempts.Add(1) == 1 {
			return errors.New("temporary")
		}
		return nil
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, RetryDelay: 10 * time.Millisecond, AckTimeout: 50 * time.Millisecond}))
	require.NoError(t, n.Handle("fail", func(ctx context.Context, task Task) error {
		failAttempts.Add(1)
		return errors.New("still broken")
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, RetryDelay: 10 * time.Millisecond, AckTimeout: 50 * time.Millisecond}))
	require.NoError(t, n.Handle("fatal", func(ctx context.Context, task Task) error {
		return NonRetryable(errors.New("fatal"))
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, AckTimeout: 50 * time.Millisecond}))
	runTaskNode(t, n)

	retryID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "retry", MaxAttempts: 3})
	require.NoError(t, err)
	failID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "fail", MaxAttempts: 2})
	require.NoError(t, err)
	fatalID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "fatal", MaxAttempts: 3})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		task, _ := n.GetTask(context.Background(), retryID)
		return task != nil && task.Status == TaskSucceeded && task.Attempt == 2
	}, 3*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		task, _ := n.GetTask(context.Background(), failID)
		return task != nil && task.Status == TaskFailed && task.Attempt == 2 && failAttempts.Load() == 2
	}, 3*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		task, _ := n.GetTask(context.Background(), fatalID)
		return task != nil && task.Status == TaskFailed && task.Attempt == 1 && task.LastError == "fatal"
	}, 3*time.Second, 20*time.Millisecond)
}

func TestHandleAfterRunStartsWorker(t *testing.T) {
	n := newTaskNode(t, "late-worker")
	runTaskNode(t, n)
	waitLeader(t, n)

	require.NoError(t, n.Handle("late", func(ctx context.Context, task Task) error {
		return nil
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, AckTimeout: 50 * time.Millisecond}))
	id, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "late"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		task, err := n.GetTask(context.Background(), id)
		return err == nil && task.Status == TaskSucceeded
	}, 3*time.Second, 20*time.Millisecond)
}

func TestExpiredClaimRecovery(t *testing.T) {
	n := newTaskNode(t, "claim-recovery")
	id, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", MaxAttempts: 2})
	require.NoError(t, err)

	now := time.Now()
	require.NoError(t, n.updateTask(context.Background(), id, func(task *Task, now time.Time) error {
		task.Status = TaskRunning
		task.Attempt = 1
		task.WorkerNodeID = "old"
		task.WorkerSessionID = "old-session"
		task.ClaimToken = "old-claim"
		task.ClaimExpiresAt = now.Add(-time.Second)
		return nil
	}))
	require.NoError(t, n.Handle("jobs", func(ctx context.Context, task Task) error {
		return nil
	}, WorkerOptions{PollInterval: 10 * time.Millisecond, RetryDelay: 10 * time.Millisecond, AckTimeout: 50 * time.Millisecond}))
	runTaskNode(t, n)

	require.Eventually(t, func() bool {
		task, err := n.GetTask(context.Background(), id)
		return err == nil && task.Status == TaskSucceeded && task.Attempt == 2 && task.StartedAt.After(now)
	}, 3*time.Second, 20*time.Millisecond)
}

func TestTaskStatsAndPrune(t *testing.T) {
	n := newTaskNode(t, "stats-prune")
	succeededID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", DedupKey: "done"})
	require.NoError(t, err)
	pendingID, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
	require.NoError(t, err)
	require.NoError(t, n.updateTask(context.Background(), succeededID, func(task *Task, now time.Time) error {
		task.Status = TaskSucceeded
		task.FinishedAt = now.Add(-time.Hour)
		return nil
	}))

	stats, err := n.TaskStats(context.Background(), "jobs")
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.Pending)
	require.Equal(t, int64(1), stats.Succeeded)
	require.Equal(t, int64(2), stats.Total)

	_, err = n.PruneTasks(context.Background(), PruneTaskOptions{})
	require.ErrorIs(t, err, ErrInvalidTask)
	pruned, err := n.PruneTasks(context.Background(), PruneTaskOptions{
		Topic:  "jobs",
		Before: time.Now(),
		Limit:  10,
	})
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	_, err = n.GetTask(context.Background(), succeededID)
	require.ErrorIs(t, err, ErrTaskNotFound)
	_, err = n.GetTask(context.Background(), pendingID)
	require.NoError(t, err)

	recreated, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs", DedupKey: "done"})
	require.NoError(t, err)
	require.NotEqual(t, succeededID, recreated)
}

func TestCancelAndRequeueInvalidStates(t *testing.T) {
	n := newTaskNode(t, "invalid-states")
	_, err := n.GetTask(context.Background(), "missing")
	require.ErrorIs(t, err, ErrTaskNotFound)

	id, err := n.SubmitTask(context.Background(), SubmitTask{Topic: "jobs"})
	require.NoError(t, err)
	require.NoError(t, n.updateTask(context.Background(), id, func(task *Task, now time.Time) error {
		task.Status = TaskRunning
		task.ClaimToken = "claim"
		return nil
	}))
	require.ErrorIs(t, n.RequeueTask(context.Background(), id, RequeueOptions{}), ErrTaskClaimLost)

	require.NoError(t, n.CancelTask(context.Background(), id))
	task, err := n.GetTask(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, TaskCancelRequested, task.Status)
	require.NoError(t, n.CancelTask(context.Background(), id))
}
