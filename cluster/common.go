// Package cluster provides lightweight KV-backed cluster coordination.
//
// It uses only kv.KV for member heartbeats, Raft-style leader election, leader
// lease fencing, and small cooperative task claims. The election model is
// intentionally not a full Raft implementation: it has terms, votes, majority
// quorum, and leader leases, but no replicated log or snapshots.
//
// Example:
//
//	store, _ := kv.NewKVFromURL("etcd://127.0.0.1:2379?prefix=app")
//	defer store.Close()
//
//	n, _ := cluster.New(cluster.Config{
//		ClusterID:    "scheduler",
//		NodeID:       "node-1",
//		Store:        store,
//		StaticVoters: []string{"node-1", "node-2", "node-3"},
//	})
//
//	n.OnStartedLeading(func(ctx context.Context, lease cluster.LeaderLease) error {
//		// Run leader-only work until ctx is canceled.
//		<-ctx.Done()
//		return nil
//	})
//
//	_ = n.Run(ctx)
package cluster

import (
	"context"
	"errors"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

// MemberMode controls whether a member participates in quorum.
type MemberMode string

const (
	// MemberVoter can vote and become leader when it is part of the voter set.
	MemberVoter MemberMode = "voter"
	// MemberObserver publishes heartbeats and can run workers, but never votes or leads.
	MemberObserver MemberMode = "observer"
)

// NodePhase describes a member lifecycle phase.
type NodePhase string

const (
	// PhaseJoining is used while a member is publishing its first heartbeats.
	PhaseJoining NodePhase = "joining"
	// PhaseActive means the member can participate in elections and work.
	PhaseActive NodePhase = "active"
	// PhaseDraining means the member is stopping workers and releasing leadership.
	PhaseDraining NodePhase = "draining"
	// PhaseLeaving is written while the member is removing itself from KV.
	PhaseLeaving NodePhase = "leaving"
	// PhaseDead is the final local state after Run has returned.
	PhaseDead NodePhase = "dead"
)

// Role is the node's local election role.
type Role string

const (
	// RoleFollower observes elections and leaders.
	RoleFollower Role = "follower"
	// RoleCandidate is campaigning for leadership in the current term.
	RoleCandidate Role = "candidate"
	// RoleLeader owns the current leader lease.
	RoleLeader Role = "leader"
)

// Config controls cluster coordination.
//
// ClusterID and NodeID are stored in KV keys and must not contain "/". For a
// multi-process cluster, prefer StaticVoters with an odd number of voters and run
// extra processes as MemberObserver so they do not affect quorum.
type Config struct {
	// ClusterID scopes all member, election, leader, and task records.
	ClusterID string
	// NodeID identifies this process within the cluster.
	NodeID string
	// Mode defaults to MemberVoter.
	Mode MemberMode
	// Store is the KV backend used for coordination.
	Store kv.KV

	// StaticVoters fixes the voter set. When set, only listed nodes can vote or lead.
	StaticVoters []string

	// HeartbeatInterval controls how often member records are renewed.
	HeartbeatInterval time.Duration
	// MemberTTL is the TTL used for member heartbeat records.
	MemberTTL time.Duration
	// LeaderLeaseTTL is the TTL used for the leader lease.
	LeaderLeaseTTL time.Duration
	// ElectionTimeoutMin is the lower bound for randomized campaigns.
	ElectionTimeoutMin time.Duration
	// ElectionTimeoutMax is the upper bound for randomized campaigns.
	ElectionTimeoutMax time.Duration
	// StabilizationWindow delays election participation after startup.
	StabilizationWindow time.Duration
	// DrainTimeout bounds shutdown waits for workers and callbacks.
	DrainTimeout time.Duration

	// Labels are stored in the member record for callers that inspect membership.
	Labels map[string]string
}

// Member is the heartbeat-backed membership record stored in KV.
type Member struct {
	NodeID    string            `json:"node_id"`
	SessionID string            `json:"session_id"`
	Mode      MemberMode        `json:"mode"`
	Phase     NodePhase         `json:"phase"`
	Labels    map[string]string `json:"labels,omitempty"`
	StartedAt time.Time         `json:"started_at"`
	RenewedAt time.Time         `json:"renewed_at"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// LeaderLease is the current leadership record.
//
// FencingToken should be copied into leader-only side effects. If an old leader
// resumes after losing its lease, writes guarded by the newer token can reject it.
type LeaderLease struct {
	ClusterID       string    `json:"cluster_id"`
	HolderNodeID    string    `json:"holder_node_id"`
	HolderSessionID string    `json:"holder_session_id"`
	Term            uint64    `json:"term"`
	Epoch           uint64    `json:"epoch"`
	FencingToken    string    `json:"fencing_token"`
	AcquiredAt      time.Time `json:"acquired_at"`
	RenewedAt       time.Time `json:"renewed_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

// Status is a point-in-time view of local state plus observed cluster state.
type Status struct {
	NodeID     string       `json:"node_id"`
	SessionID  string       `json:"session_id"`
	Phase      NodePhase    `json:"phase"`
	Role       Role         `json:"role"`
	Leader     *LeaderLease `json:"leader,omitempty"`
	Members    []Member     `json:"members"`
	Conditions []Condition  `json:"conditions"`
}

// Condition describes a boolean status signal included in Status.
type Condition struct {
	Type    string    `json:"type"`
	Status  bool      `json:"status"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Time    time.Time `json:"time"`
}

// LeadingFunc runs leader-only work.
//
// The context is canceled when leadership is lost or the node is draining. If
// the function returns a non-nil error before the context is canceled, the node
// releases leadership.
type LeadingFunc func(context.Context, LeaderLease) error

// StoppedLeadingFunc is called after local leadership is stopped.
type StoppedLeadingFunc func(LeaderLease)

var (
	ErrNotLeader     = errors.New("cluster: not leader")
	ErrNoQuorum      = errors.New("cluster: no quorum")
	ErrNodeClosed    = errors.New("cluster: closed")
	ErrFenced        = errors.New("cluster: leadership fenced")
	ErrInvalidConfig = errors.New("cluster: invalid config")
	ErrTaskNotFound  = errors.New("cluster: task not found")
	ErrTaskFinalized = errors.New("cluster: task finalized")
	ErrTaskClaimLost = errors.New("cluster: task claim lost")
	ErrTaskCanceled  = errors.New("cluster: task canceled")
	ErrInvalidTask   = errors.New("cluster: invalid task")
)
