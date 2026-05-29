package cluster

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.d7z.net/middleware/kv"
)

// Node is one process participating in a cluster.
//
// Create a Node with New, register any leader callbacks or task handlers, and
// call Run. Run blocks until the passed context is canceled or Close is called.
// A Node is intentionally not reusable after Run returns.
type Node struct {
	cfg       Config
	root      kv.KV
	sessionID string
	startedAt time.Time

	mu           sync.RWMutex
	started      bool
	phase        NodePhase
	role         Role
	currentTerm  uint64
	leader       *LeaderLease
	leaderRaw    string
	leaderCancel context.CancelFunc

	leadingMu sync.RWMutex
	leading   []LeadingFunc
	stopped   []StoppedLeadingFunc

	cancel    context.CancelFunc
	closeOnce sync.Once
	closeCh   chan struct{}
	loopWG    sync.WaitGroup

	workerMu       sync.Mutex
	workers        []workerRegistration
	workerCtx      context.Context
	workerCancel   context.CancelFunc
	workerLoopWG   sync.WaitGroup
	workerActiveWG sync.WaitGroup
}

type campaign struct {
	ClusterID  string    `json:"cluster_id"`
	NodeID     string    `json:"node_id"`
	SessionID  string    `json:"session_id"`
	Term       uint64    `json:"term"`
	StartedAt  time.Time `json:"started_at"`
	ValidUntil time.Time `json:"valid_until"`
}

type vote struct {
	Term               uint64    `json:"term"`
	VoterNodeID        string    `json:"voter_node_id"`
	VoterSessionID     string    `json:"voter_session_id"`
	CandidateNodeID    string    `json:"candidate_node_id"`
	CandidateSessionID string    `json:"candidate_session_id"`
	VotedAt            time.Time `json:"voted_at"`
}

type quorumState struct {
	voters       map[string]struct{}
	activeVoters map[string]struct{}
	quorum       int
	hasQuorum    bool
}

// New validates Config and creates a Node.
//
// New does not write to KV or start goroutines. Call Run to join the cluster.
func New(cfg Config) (*Node, error) {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	sessionID, err := randomID("ses", 18)
	if err != nil {
		return nil, err
	}

	return &Node{
		cfg:       normalized,
		root:      normalized.Store.Child("cluster", normalized.ClusterID),
		sessionID: sessionID,
		startedAt: time.Now(),
		phase:     PhaseJoining,
		role:      RoleFollower,
		closeCh:   make(chan struct{}),
	}, nil
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.ClusterID == "" || cfg.NodeID == "" || cfg.Store == nil {
		return Config{}, ErrInvalidConfig
	}
	if invalidKey(cfg.ClusterID) {
		return Config{}, fmt.Errorf("%w: cluster id must not be empty or contain '/'", ErrInvalidConfig)
	}
	if invalidKey(cfg.NodeID) {
		return Config{}, fmt.Errorf("%w: node id must not be empty or contain '/'", ErrInvalidConfig)
	}
	for _, id := range cfg.StaticVoters {
		if invalidKey(id) {
			return Config{}, fmt.Errorf("%w: static voter id must not be empty or contain '/'", ErrInvalidConfig)
		}
	}
	if cfg.Mode == "" {
		cfg.Mode = MemberVoter
	}
	if cfg.Mode != MemberVoter && cfg.Mode != MemberObserver {
		return Config{}, fmt.Errorf("%w: unsupported member mode %q", ErrInvalidConfig, cfg.Mode)
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.MemberTTL <= 0 {
		cfg.MemberTTL = 10 * time.Second
	}
	if cfg.LeaderLeaseTTL <= 0 {
		cfg.LeaderLeaseTTL = 6 * time.Second
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 3 * time.Second
	}
	if cfg.ElectionTimeoutMax <= 0 {
		cfg.ElectionTimeoutMax = 6 * time.Second
	}
	if cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		return Config{}, fmt.Errorf("%w: election timeout max is less than min", ErrInvalidConfig)
	}
	if cfg.StabilizationWindow < 0 {
		return Config{}, fmt.Errorf("%w: stabilization window must not be negative", ErrInvalidConfig)
	}
	if cfg.StabilizationWindow == 0 {
		cfg.StabilizationWindow = 2 * cfg.HeartbeatInterval
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	if cfg.Labels == nil {
		cfg.Labels = map[string]string{}
	}
	return cfg, nil
}

func invalidKey(v string) bool {
	return strings.TrimSpace(v) == "" || strings.Contains(v, "/")
}

// Run joins the cluster, starts heartbeats, elections, and workers, then blocks.
//
// On shutdown it drains workers, releases leadership if owned, removes the
// member record, and returns. Run may be called only once.
func (n *Node) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return ErrNodeClosed
	}
	n.started = true
	runCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.mu.Unlock()

	if err := n.setPhase(runCtx, PhaseJoining); err != nil {
		cancel()
		return err
	}

	n.loopWG.Add(1)
	go func() {
		defer n.loopWG.Done()
		n.heartbeatLoop(runCtx)
	}()

	if err := n.wait(runCtx, n.cfg.StabilizationWindow); err != nil {
		cancel()
		return err
	}
	if err := n.setPhase(runCtx, PhaseActive); err != nil {
		cancel()
		return err
	}

	if n.cfg.Mode == MemberVoter {
		n.loopWG.Add(1)
		go func() {
			defer n.loopWG.Done()
			n.electionLoop(runCtx)
		}()
	}
	n.startWorkers(runCtx)

	select {
	case <-runCtx.Done():
	case <-n.closeCh:
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), n.cfg.DrainTimeout)
	_ = n.Drain(drainCtx)
	drainCancel()
	cancel()
	n.loopWG.Wait()
	_ = n.setPhase(context.Background(), PhaseLeaving)
	_, _ = n.root.Child("members").Delete(context.Background(), n.cfg.NodeID)
	n.setLocalPhase(PhaseDead)
	return nil
}

// Close requests Run to stop.
//
// Close is safe to call multiple times. It does not wait for Run to return; the
// caller should wait on the goroutine running Run if graceful completion matters.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.mu.RLock()
		cancel := n.cancel
		n.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
		close(n.closeCh)
	})
	return nil
}

// Drain stops accepting new local work and releases leadership if this node owns it.
//
// Drain is useful before process shutdown or maintenance. In-flight task
// handlers are allowed to finish until the configured drain timeout.
func (n *Node) Drain(ctx context.Context) error {
	if err := n.setPhase(ctx, PhaseDraining); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	n.stopWorkers(ctx)
	n.stopLeadership(ctx, true)
	return nil
}

// ID returns the configured node ID.
func (n *Node) ID() string {
	return n.cfg.NodeID
}

// SessionID returns the unique process session ID assigned by New.
func (n *Node) SessionID() string {
	return n.sessionID
}

// Phase returns the node's current local lifecycle phase.
func (n *Node) Phase() NodePhase {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.phase
}

// Role returns the node's current local election role.
func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

// IsLeader reports whether this node currently owns an unexpired leader lease.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == RoleLeader && n.leader != nil && time.Now().Before(n.leader.ExpiresAt)
}

// Leader returns the currently observed unexpired leader lease.
//
// If there is no active leader, Leader returns kv.ErrKeyNotFound.
func (n *Node) Leader(ctx context.Context) (*LeaderLease, error) {
	lease, _, ok, err := n.readLeader(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, kv.ErrKeyNotFound
	}
	return lease, nil
}

// Members returns non-expired member records observed in KV.
func (n *Node) Members(ctx context.Context) ([]Member, error) {
	return n.readMembers(ctx)
}

// Status returns local state, observed members, observed leader, and conditions.
func (n *Node) Status(ctx context.Context) (*Status, error) {
	members, err := n.readMembers(ctx)
	if err != nil {
		return nil, err
	}
	leader, _, leaderOK, err := n.readLeader(ctx)
	if err != nil {
		return nil, err
	}
	quorum := n.quorum(members)

	n.mu.RLock()
	status := &Status{
		NodeID:    n.cfg.NodeID,
		SessionID: n.sessionID,
		Phase:     n.phase,
		Role:      n.role,
		Members:   members,
	}
	if leaderOK {
		status.Leader = leader
	}
	n.mu.RUnlock()

	now := time.Now()
	status.Conditions = append(status.Conditions,
		Condition{Type: "HasQuorum", Status: quorum.hasQuorum, Time: now},
		Condition{Type: "MemberLeaseRenewing", Status: n.memberActive(members), Time: now},
		Condition{Type: "LeaderLeaseRenewing", Status: n.IsLeader(), Time: now},
		Condition{Type: "Draining", Status: status.Phase == PhaseDraining, Time: now},
	)
	return status, nil
}

// OnStartedLeading registers a callback that starts when this node becomes leader.
//
// Register callbacks before Run when possible. Callbacks registered after Run
// are used for future leadership transitions.
func (n *Node) OnStartedLeading(fn LeadingFunc) {
	if fn == nil {
		return
	}
	n.leadingMu.Lock()
	defer n.leadingMu.Unlock()
	n.leading = append(n.leading, fn)
}

// OnStoppedLeading registers a callback that runs after this node stops leading.
func (n *Node) OnStoppedLeading(fn StoppedLeadingFunc) {
	if fn == nil {
		return
	}
	n.leadingMu.Lock()
	defer n.leadingMu.Unlock()
	n.stopped = append(n.stopped, fn)
}

func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			phase := n.Phase()
			if phase == PhaseDead || phase == PhaseLeaving {
				continue
			}
			_ = n.writeMember(ctx, phase)
		}
	}
}

func (n *Node) electionLoop(ctx context.Context) {
	deadline := time.Now().Add(n.randomElectionTimeout())
	tick := n.cfg.HeartbeatInterval / 2
	if tick <= 0 || tick > 500*time.Millisecond {
		tick = 500 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if n.Phase() != PhaseActive {
			deadline = time.Now().Add(n.randomElectionTimeout())
			continue
		}

		leader, raw, leaderOK, err := n.readLeader(ctx)
		if err != nil {
			continue
		}
		if leaderOK {
			n.observeLeader(leader, raw)
			if leader.HolderNodeID == n.cfg.NodeID && leader.HolderSessionID == n.sessionID && n.Role() == RoleLeader {
				if !n.renewLeader(ctx) {
					deadline = time.Now().Add(n.randomElectionTimeout())
				}
			} else {
				deadline = time.Now().Add(n.randomElectionTimeout())
			}
			continue
		}

		if n.Role() == RoleLeader {
			n.stopLeadership(ctx, false)
			deadline = time.Now().Add(n.randomElectionTimeout())
		}

		n.castVote(ctx)
		if time.Now().Before(deadline) {
			continue
		}
		_ = n.campaign(ctx)
		deadline = time.Now().Add(n.randomElectionTimeout())
	}
}

func (n *Node) campaign(ctx context.Context) error {
	members, err := n.readMembers(ctx)
	if err != nil {
		return err
	}
	quorum := n.quorum(members)
	if !quorum.hasQuorum {
		return ErrNoQuorum
	}
	if _, ok := quorum.voters[n.cfg.NodeID]; !ok {
		return ErrNoQuorum
	}

	term, err := n.advanceCounter(ctx, n.root.Child("terms"), "current")
	if err != nil {
		return err
	}
	n.setRole(RoleCandidate, term)

	now := time.Now()
	c := campaign{
		ClusterID:  n.cfg.ClusterID,
		NodeID:     n.cfg.NodeID,
		SessionID:  n.sessionID,
		Term:       term,
		StartedAt:  now,
		ValidUntil: now.Add(n.cfg.ElectionTimeoutMax + n.cfg.LeaderLeaseTTL),
	}
	raw, err := marshalString(c)
	if err != nil {
		return err
	}
	if err := n.root.Child("terms", termString(term), "campaigns").Put(ctx, n.sessionID, raw, n.cfg.ElectionTimeoutMax+n.cfg.LeaderLeaseTTL); err != nil {
		return err
	}
	_, _ = n.root.Child("terms", termString(term), "votes").PutIfNotExists(ctx, n.cfg.NodeID, n.voteRaw(term, c), n.cfg.ElectionTimeoutMax+n.cfg.LeaderLeaseTTL)

	poll := n.cfg.HeartbeatInterval / 4
	if poll <= 0 || poll > 200*time.Millisecond {
		poll = 200 * time.Millisecond
	}
	timeout := time.NewTimer(n.cfg.ElectionTimeoutMax)
	defer timeout.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	for {
		if leader, _, ok, err := n.readLeader(ctx); err != nil {
			return err
		} else if ok {
			n.observeLeader(leader, "")
			return nil
		}

		current, err := n.readCurrentTerm(ctx)
		if err != nil {
			return err
		}
		if current > term {
			n.setRole(RoleFollower, current)
			return nil
		}

		count, err := n.countVotes(ctx, term, quorum.voters)
		if err != nil {
			return err
		}
		if count >= quorum.quorum {
			return n.acquireLeadership(ctx, term)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			n.setRole(RoleFollower, term)
			return nil
		case <-ticker.C:
		}
	}
}

func (n *Node) castVote(ctx context.Context) {
	leader, _, ok, err := n.readLeader(ctx)
	if err != nil || ok && leader != nil {
		return
	}
	term, err := n.readCurrentTerm(ctx)
	if err != nil || term == 0 {
		return
	}

	votes := n.root.Child("terms", termString(term), "votes")
	if _, err := votes.Get(ctx, n.cfg.NodeID); err == nil {
		return
	}

	pairs, err := n.root.Child("terms", termString(term), "campaigns").List(ctx, "")
	if err != nil || len(pairs) == 0 {
		return
	}
	members, err := n.readMembers(ctx)
	if err != nil {
		return
	}
	quorum := n.quorum(members)
	if !quorum.hasQuorum {
		return
	}
	if _, ok := quorum.voters[n.cfg.NodeID]; !ok {
		return
	}
	active := make(map[string]Member, len(members))
	for _, member := range members {
		if member.Mode == MemberVoter && member.Phase == PhaseActive {
			active[member.NodeID] = member
		}
	}

	campaigns := make([]campaign, 0, len(pairs))
	now := time.Now()
	for _, pair := range pairs {
		var c campaign
		if json.Unmarshal([]byte(pair.Value), &c) != nil || c.Term != term || now.After(c.ValidUntil) {
			continue
		}
		if _, ok := quorum.voters[c.NodeID]; !ok {
			continue
		}
		if member, ok := active[c.NodeID]; !ok || member.SessionID != c.SessionID {
			continue
		}
		campaigns = append(campaigns, c)
	}
	if len(campaigns) == 0 {
		return
	}
	sort.Slice(campaigns, func(i, j int) bool {
		if !campaigns[i].StartedAt.Equal(campaigns[j].StartedAt) {
			return campaigns[i].StartedAt.Before(campaigns[j].StartedAt)
		}
		if campaigns[i].NodeID != campaigns[j].NodeID {
			return campaigns[i].NodeID < campaigns[j].NodeID
		}
		return campaigns[i].SessionID < campaigns[j].SessionID
	})

	selected := campaigns[0]
	_, _ = votes.PutIfNotExists(ctx, n.cfg.NodeID, n.voteRaw(term, selected), n.cfg.ElectionTimeoutMax+n.cfg.LeaderLeaseTTL)
	n.setLocalTerm(term)
}

func (n *Node) acquireLeadership(ctx context.Context, term uint64) error {
	if leader, _, ok, err := n.readLeader(ctx); err != nil {
		return err
	} else if ok && leader != nil {
		n.observeLeader(leader, "")
		return nil
	}

	epoch, err := n.advanceCounter(ctx, n.root, "epoch")
	if err != nil {
		return err
	}
	now := time.Now()
	lease := LeaderLease{
		ClusterID:       n.cfg.ClusterID,
		HolderNodeID:    n.cfg.NodeID,
		HolderSessionID: n.sessionID,
		Term:            term,
		Epoch:           epoch,
		FencingToken:    fmt.Sprintf("%s/%d/%d/%s/%s", n.cfg.ClusterID, term, epoch, n.cfg.NodeID, n.sessionID),
		AcquiredAt:      now,
		RenewedAt:       now,
		ExpiresAt:       now.Add(n.cfg.LeaderLeaseTTL),
	}
	raw, err := marshalString(lease)
	if err != nil {
		return err
	}

	oldRaw, err := n.root.Get(ctx, "leader")
	var ok bool
	switch {
	case errors.Is(err, kv.ErrKeyNotFound):
		ok, err = n.root.PutIfNotExists(ctx, "leader", raw, n.cfg.LeaderLeaseTTL)
	case err != nil:
		return err
	default:
		var old LeaderLease
		if json.Unmarshal([]byte(oldRaw), &old) == nil && time.Now().Before(old.ExpiresAt) {
			n.observeLeader(&old, oldRaw)
			return nil
		}
		ok, err = n.root.CompareAndSwapTTL(ctx, "leader", oldRaw, raw, n.cfg.LeaderLeaseTTL)
	}
	if err != nil {
		return err
	}
	if !ok {
		n.setRole(RoleFollower, term)
		return nil
	}

	n.startLeadership(lease, raw)
	return nil
}

func (n *Node) renewLeader(ctx context.Context) bool {
	n.mu.RLock()
	if n.leader == nil || n.leaderRaw == "" {
		n.mu.RUnlock()
		return false
	}
	lease := *n.leader
	oldRaw := n.leaderRaw
	n.mu.RUnlock()

	now := time.Now()
	lease.RenewedAt = now
	lease.ExpiresAt = now.Add(n.cfg.LeaderLeaseTTL)
	newRaw, err := marshalString(lease)
	if err != nil {
		return false
	}
	ok, err := n.root.CompareAndSwapTTL(ctx, "leader", oldRaw, newRaw, n.cfg.LeaderLeaseTTL)
	if err != nil || !ok {
		n.stopLeadership(ctx, false)
		return false
	}

	n.mu.Lock()
	n.leader = &lease
	n.leaderRaw = newRaw
	n.currentTerm = maxUint64(n.currentTerm, lease.Term)
	n.mu.Unlock()
	return true
}

func (n *Node) startLeadership(lease LeaderLease, raw string) {
	leaderCtx, cancel := context.WithCancel(context.Background())

	n.mu.Lock()
	if n.leaderCancel != nil {
		n.leaderCancel()
	}
	n.role = RoleLeader
	n.currentTerm = maxUint64(n.currentTerm, lease.Term)
	n.leader = &lease
	n.leaderRaw = raw
	n.leaderCancel = cancel
	n.mu.Unlock()

	n.leadingMu.RLock()
	callbacks := append([]LeadingFunc(nil), n.leading...)
	n.leadingMu.RUnlock()
	for _, fn := range callbacks {
		fn := fn
		n.loopWG.Add(1)
		go func() {
			defer n.loopWG.Done()
			if err := fn(leaderCtx, lease); err != nil && leaderCtx.Err() == nil {
				n.stopLeadership(context.Background(), true)
			}
		}()
	}
}

func (n *Node) stopLeadership(ctx context.Context, release bool) {
	n.mu.Lock()
	if n.role != RoleLeader && n.leader == nil {
		n.mu.Unlock()
		return
	}
	var lease LeaderLease
	if n.leader != nil {
		lease = *n.leader
	}
	oldRaw := n.leaderRaw
	if n.leaderCancel != nil {
		n.leaderCancel()
	}
	n.role = RoleFollower
	n.leader = nil
	n.leaderRaw = ""
	n.leaderCancel = nil
	n.mu.Unlock()

	if release && oldRaw != "" {
		_, _ = n.root.DeleteIfValue(ctx, "leader", oldRaw)
	}

	n.leadingMu.RLock()
	callbacks := append([]StoppedLeadingFunc(nil), n.stopped...)
	n.leadingMu.RUnlock()
	for _, fn := range callbacks {
		fn(lease)
	}
}

func (n *Node) observeLeader(lease *LeaderLease, raw string) {
	if lease == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = maxUint64(n.currentTerm, lease.Term)
	if lease.HolderNodeID == n.cfg.NodeID && lease.HolderSessionID == n.sessionID && n.role == RoleLeader {
		if raw != "" {
			leaseCopy := *lease
			n.leader = &leaseCopy
			n.leaderRaw = raw
		}
		return
	}
	if n.leaderCancel != nil {
		n.leaderCancel()
	}
	n.role = RoleFollower
	leaseCopy := *lease
	n.leader = &leaseCopy
	n.leaderRaw = raw
	n.leaderCancel = nil
}

func (n *Node) setPhase(ctx context.Context, phase NodePhase) error {
	n.setLocalPhase(phase)
	return n.writeMember(ctx, phase)
}

func (n *Node) setLocalPhase(phase NodePhase) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.phase = phase
}

func (n *Node) setRole(role Role, term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = role
	n.currentTerm = maxUint64(n.currentTerm, term)
}

func (n *Node) setLocalTerm(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = maxUint64(n.currentTerm, term)
}

func (n *Node) writeMember(ctx context.Context, phase NodePhase) error {
	now := time.Now()
	member := Member{
		NodeID:    n.cfg.NodeID,
		SessionID: n.sessionID,
		Mode:      n.cfg.Mode,
		Phase:     phase,
		Labels:    n.cfg.Labels,
		StartedAt: n.startedAt,
		RenewedAt: now,
		ExpiresAt: now.Add(n.cfg.MemberTTL),
	}
	raw, err := marshalString(member)
	if err != nil {
		return err
	}
	return n.root.Child("members").Put(ctx, n.cfg.NodeID, raw, n.cfg.MemberTTL)
}

func (n *Node) readMembers(ctx context.Context) ([]Member, error) {
	pairs, err := n.root.Child("members").List(ctx, "")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	members := make([]Member, 0, len(pairs))
	for _, pair := range pairs {
		var member Member
		if err := json.Unmarshal([]byte(pair.Value), &member); err != nil {
			continue
		}
		if now.After(member.ExpiresAt) || member.Phase == PhaseDead || member.Phase == PhaseLeaving {
			continue
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].NodeID < members[j].NodeID
	})
	return members, nil
}

func (n *Node) memberActive(members []Member) bool {
	for _, member := range members {
		if member.NodeID == n.cfg.NodeID && member.SessionID == n.sessionID && member.Phase != PhaseDead {
			return true
		}
	}
	return false
}

func (n *Node) readLeader(ctx context.Context) (*LeaderLease, string, bool, error) {
	raw, err := n.root.Get(ctx, "leader")
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return nil, "", false, nil
		}
		return nil, "", false, err
	}
	var lease LeaderLease
	if err := json.Unmarshal([]byte(raw), &lease); err != nil {
		return nil, raw, false, err
	}
	if time.Now().After(lease.ExpiresAt) {
		return &lease, raw, false, nil
	}
	return &lease, raw, true, nil
}

func (n *Node) quorum(members []Member) quorumState {
	active := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.Mode == MemberVoter && member.Phase == PhaseActive {
			active[member.NodeID] = struct{}{}
		}
	}

	voters := make(map[string]struct{})
	if len(n.cfg.StaticVoters) > 0 {
		for _, id := range n.cfg.StaticVoters {
			voters[id] = struct{}{}
		}
	} else {
		for id := range active {
			voters[id] = struct{}{}
		}
	}
	if _, ok := voters[n.cfg.NodeID]; !ok && n.cfg.Mode == MemberVoter && len(n.cfg.StaticVoters) == 0 {
		voters[n.cfg.NodeID] = struct{}{}
	}

	activeVoters := make(map[string]struct{})
	for id := range voters {
		if _, ok := active[id]; ok {
			activeVoters[id] = struct{}{}
		}
	}
	quorum := len(voters)/2 + 1
	if len(voters) == 0 {
		quorum = 0
	}
	return quorumState{
		voters:       voters,
		activeVoters: activeVoters,
		quorum:       quorum,
		hasQuorum:    quorum > 0 && len(activeVoters) >= quorum,
	}
}

func (n *Node) readCurrentTerm(ctx context.Context) (uint64, error) {
	raw, err := n.root.Child("terms").Get(ctx, "current")
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	term, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	n.setLocalTerm(term)
	return term, nil
}

func (n *Node) advanceCounter(ctx context.Context, store kv.KV, key string) (uint64, error) {
	for attempt := 0; attempt < 16; attempt++ {
		raw, err := store.Get(ctx, key)
		if errors.Is(err, kv.ErrKeyNotFound) {
			if ok, putErr := store.PutIfNotExists(ctx, key, "1", kv.TTLKeep); putErr != nil {
				return 0, putErr
			} else if ok {
				return 1, nil
			}
			continue
		}
		if err != nil {
			return 0, err
		}
		current, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, err
		}
		next := current + 1
		ok, err := store.CompareAndSwap(ctx, key, raw, strconv.FormatUint(next, 10))
		if err != nil {
			return 0, err
		}
		if ok {
			return next, nil
		}
		if err := n.wait(ctx, 10*time.Millisecond); err != nil {
			return 0, err
		}
	}
	return 0, ErrNoQuorum
}

func (n *Node) voteRaw(term uint64, c campaign) string {
	raw, _ := marshalString(vote{
		Term:               term,
		VoterNodeID:        n.cfg.NodeID,
		VoterSessionID:     n.sessionID,
		CandidateNodeID:    c.NodeID,
		CandidateSessionID: c.SessionID,
		VotedAt:            time.Now(),
	})
	return raw
}

func (n *Node) countVotes(ctx context.Context, term uint64, voters map[string]struct{}) (int, error) {
	pairs, err := n.root.Child("terms", termString(term), "votes").List(ctx, "")
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		if _, ok := voters[pair.Key]; !ok {
			continue
		}
		var v vote
		if err := json.Unmarshal([]byte(pair.Value), &v); err != nil {
			continue
		}
		if v.Term == term &&
			v.VoterNodeID == pair.Key &&
			v.CandidateNodeID == n.cfg.NodeID &&
			v.CandidateSessionID == n.sessionID {
			seen[pair.Key] = struct{}{}
		}
	}
	return len(seen), nil
}

func (n *Node) randomElectionTimeout() time.Duration {
	minDelay := n.cfg.ElectionTimeoutMin
	window := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	if window <= 0 {
		return minDelay
	}
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(window)))
	if err != nil {
		return minDelay
	}
	return minDelay + time.Duration(nBig.Int64())
}

func (n *Node) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.closeCh:
		return ErrNodeClosed
	case <-timer.C:
		return nil
	}
}

func marshalString(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func randomID(prefix string, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func termString(term uint64) string {
	return strconv.FormatUint(term, 10)
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
