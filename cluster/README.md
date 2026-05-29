# cluster

`cluster` provides lightweight cluster coordination on top of `kv.KV`.

It intentionally depends only on `kv`; `queue` and `subscribe` are not part of
the design. The package is useful for internal controllers, schedulers, and
worker processes that need one active leader and a small amount of cooperative
task handling across a cluster.

## Scope

The election model is Raft-inspired, not a full Raft implementation. It includes:

- member heartbeat with TTL
- voter / observer modes
- term and vote records
- majority quorum
- randomized election timeout
- leader lease renewal
- fencing token for leader-only work

It does not implement replicated logs, snapshots, dynamic Raft membership, or
multi-group consensus.

## Storage Requirements

The backing `kv.KV` must provide atomic compare-and-swap semantics and TTL
support. The package uses:

- `Put`
- `Get`
- `PutIfNotExists`
- `CompareAndSwap`
- `CompareAndSwapTTL`
- `DeleteIfValue`
- `List`

The in-memory backend is suitable for tests and single-process use. A shared
linearizable backend such as etcd should be used when multiple processes
coordinate through the same cluster.

## Basic Usage

```go
store, _ := kv.NewKVFromURL("etcd://127.0.0.1:2379?prefix=app")
defer store.Close()

n, _ := cluster.New(cluster.Config{
    ClusterID:    "scheduler",
    NodeID:       "node-1",
    Store:        store,
    StaticVoters: []string{"node-1", "node-2", "node-3"},
})

n.OnStartedLeading(func(ctx context.Context, lease cluster.LeaderLease) error {
    // Run leader-only reconcile loops here. Stop when ctx is canceled.
    <-ctx.Done()
    return nil
})

_ = n.Run(ctx)
```

## Quorum

Only majority quorum is supported:

```text
quorum = floor(voterCount / 2) + 1
```

Examples:

```text
1 voter  -> quorum 1
2 voters -> quorum 2
3 voters -> quorum 2
4 voters -> quorum 3
5 voters -> quorum 3
```

Even voter counts are allowed but usually not useful. A four-voter cluster has
the same failure tolerance as a three-voter cluster. A two-voter cluster cannot
elect a replacement leader when one voter is down. Prefer `1`, `3`, or `5`
voters and use `MemberObserver` for extra processes that should not affect
quorum.

`StaticVoters` is recommended for multi-node clusters because it keeps quorum
stable when nodes go offline. When `StaticVoters` is set, only listed members
can vote or become leader; any extra process should run as `MemberObserver`. If
`StaticVoters` is empty, the package derives the voter set from active member
heartbeats, which is convenient for local tests and single-process use but
weaker for production-style coordination.

## Fencing

Every leader lease has a `FencingToken`:

```go
type LeaderLease struct {
    ClusterID       string
    HolderNodeID    string
    HolderSessionID string
    Term            uint64
    Epoch           uint64
    FencingToken    string
    AcquiredAt      time.Time
    RenewedAt       time.Time
    ExpiresAt       time.Time
}
```

Leader-only side effects should record or verify this token. If an old leader
pauses and later resumes after another node has acquired leadership, its lease
renewal fails and the node is fenced.

## Lifecycle

Nodes move through these phases:

```text
joining -> active -> draining -> leaving -> dead
```

On start, a node writes a member record and waits for `StabilizationWindow`
before participating in elections. On drain or close, it stops accepting new
work, releases leadership if it owns the lease, waits for in-flight handlers,
and removes its member record.

## KV Task Model

The package includes an optional KV task claim model for small control-plane
workloads. It is not a high-throughput queue. Execution is at least once, so
handlers must be idempotent.

Tasks move through these statuses:

```text
pending -> running -> succeeded
pending -> running -> retrying -> pending
pending -> running -> failed
pending -> canceled
running -> cancel_requested -> canceled
```

```go
_ = n.Handle("jobs", func(ctx context.Context, task cluster.Task) error {
    // Handler must be idempotent.
    return nil
}, cluster.WorkerOptions{
    Concurrency: 4,
    AckTimeout:  30 * time.Second,
})

id, _ := n.SubmitTask(ctx, cluster.SubmitTask{
    Topic:       "jobs",
    Payload:     []byte("payload"),
    MaxAttempts: 3,
    LeaderOnly:  true,
})
_ = id
```

The task API is:

```go
func (n *Node) SubmitTask(ctx context.Context, req SubmitTask) (TaskID, error)
func (n *Node) GetTask(ctx context.Context, id TaskID) (*Task, error)
func (n *Node) ListTasks(ctx context.Context, opts ListTaskOptions) (*ListTaskResult, error)
func (n *Node) CancelTask(ctx context.Context, id TaskID) error
func (n *Node) RequeueTask(ctx context.Context, id TaskID, opts RequeueOptions) error
func (n *Node) TaskStats(ctx context.Context, topic string) (*TaskStats, error)
func (n *Node) PruneTasks(ctx context.Context, opts PruneTaskOptions) (int, error)
func (n *Node) Handle(topic string, handler TaskHandler, opts WorkerOptions) error
```

Workers claim tasks with a TTL-backed claim record under `tasks/claims`. If a
worker exits or misses its ack timeout, the claim expires and another worker can
recover the task. Ordinary handler errors retry until `MaxAttempts`; returning
`cluster.NonRetryable(err)` fails the task immediately. Returning
`cluster.ErrTaskCanceled` marks it canceled.

`SubmitTask` supports:

- delayed visibility with `Delay`
- duplicate submission suppression with `DedupKey`
- leader-only submission with `LeaderOnly`
- fencing token capture for leader-created work

`ListTasks`, `TaskStats`, and `PruneTasks` scan task data from KV. They are
intended for small control-plane task sets and are subject to the configured KV
list limits. `PruneTasks` only removes terminal tasks (`succeeded`, `failed`,
`canceled`) and requires a `Before` timestamp.
