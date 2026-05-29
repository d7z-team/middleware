package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/kv"
)

func testConfig(store kv.KV, cluster, id string, voters []string) Config {
	return Config{
		ClusterID:           cluster,
		NodeID:              id,
		Store:               store,
		StaticVoters:        voters,
		HeartbeatInterval:   20 * time.Millisecond,
		MemberTTL:           120 * time.Millisecond,
		LeaderLeaseTTL:      80 * time.Millisecond,
		ElectionTimeoutMin:  30 * time.Millisecond,
		ElectionTimeoutMax:  60 * time.Millisecond,
		StabilizationWindow: 20 * time.Millisecond,
		DrainTimeout:        300 * time.Millisecond,
	}
}

func startTestNode(t *testing.T, cfg Config) (*Node, context.CancelFunc, <-chan error) {
	t.Helper()
	n, err := New(cfg)
	require.NoError(t, err)

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
	return n, cancel, errCh
}

func waitLeader(t *testing.T, nodes ...*Node) *Node {
	t.Helper()
	var leader *Node
	require.Eventually(t, func() bool {
		leader = nil
		for _, n := range nodes {
			if n.IsLeader() {
				if leader != nil {
					return false
				}
				leader = n
			}
		}
		return leader != nil
	}, 3*time.Second, 20*time.Millisecond)
	return leader
}

func TestSingleNodeElection(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	n, _, _ := startTestNode(t, testConfig(store, "single", "n1", nil))
	leader := waitLeader(t, n)
	require.Equal(t, "n1", leader.ID())

	status, err := n.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, RoleLeader, status.Role)
	require.Condition(t, func() bool {
		for _, condition := range status.Conditions {
			if condition.Type == "HasQuorum" {
				return condition.Status
			}
		}
		return false
	})
}

func TestStaticTwoVotersDoNotElectWithoutQuorum(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	n, _, _ := startTestNode(t, testConfig(store, "two", "n1", []string{"n1", "n2"}))
	require.Never(t, func() bool {
		return n.IsLeader()
	}, 300*time.Millisecond, 20*time.Millisecond)

	status, err := n.Status(context.Background())
	require.NoError(t, err)
	require.Condition(t, func() bool {
		for _, condition := range status.Conditions {
			if condition.Type == "HasQuorum" {
				return !condition.Status
			}
		}
		return false
	})
}

func TestStaticFourVotersNeedThreeActiveMembers(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	voters := []string{"n1", "n2", "n3", "n4"}
	n1, _, _ := startTestNode(t, testConfig(store, "four", "n1", voters))
	n2, _, _ := startTestNode(t, testConfig(store, "four", "n2", voters))

	require.Never(t, func() bool {
		return n1.IsLeader() || n2.IsLeader()
	}, 300*time.Millisecond, 20*time.Millisecond)
}

func TestThreeVoterFailover(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)
	voters := []string{"n1", "n2", "n3"}

	n1, cancel1, done1 := startTestNode(t, testConfig(store, "failover", "n1", voters))
	n2, cancel2, done2 := startTestNode(t, testConfig(store, "failover", "n2", voters))
	n3, cancel3, done3 := startTestNode(t, testConfig(store, "failover", "n3", voters))

	first := waitLeader(t, n1, n2, n3)
	firstID := first.ID()
	switch first {
	case n1:
		cancel1()
		require.NoError(t, n1.Close())
		select {
		case <-done1:
		case <-time.After(time.Second):
			t.Fatal("leader did not stop")
		}
	case n2:
		cancel2()
		require.NoError(t, n2.Close())
		select {
		case <-done2:
		case <-time.After(time.Second):
			t.Fatal("leader did not stop")
		}
	case n3:
		cancel3()
		require.NoError(t, n3.Close())
		select {
		case <-done3:
		case <-time.After(time.Second):
			t.Fatal("leader did not stop")
		}
	}

	require.Eventually(t, func() bool {
		for _, n := range []*Node{n1, n2, n3} {
			if n.ID() != firstID && n.IsLeader() {
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond)
}

func TestObserverDoesNotVoteOrLead(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)
	voters := []string{"n1"}

	voter := testConfig(store, "observer", "n1", voters)
	observer := testConfig(store, "observer", "n2", voters)
	observer.Mode = MemberObserver

	n1, _, _ := startTestNode(t, voter)
	n2, _, _ := startTestNode(t, observer)

	require.Equal(t, n1, waitLeader(t, n1, n2))
	require.Never(t, func() bool {
		return n2.IsLeader()
	}, 250*time.Millisecond, 20*time.Millisecond)

	status, err := n1.Status(context.Background())
	require.NoError(t, err)
	require.Len(t, status.Members, 2)
}

func TestStaticVotersRejectUnlistedCandidate(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)
	voters := []string{"n1"}

	n2, _, _ := startTestNode(t, testConfig(store, "static-boundary", "n2", voters))
	n1, _, _ := startTestNode(t, testConfig(store, "static-boundary", "n1", voters))

	require.Equal(t, n1, waitLeader(t, n1, n2))
	require.Never(t, func() bool {
		return n2.IsLeader()
	}, 250*time.Millisecond, 20*time.Millisecond)
}

func TestDrainReleasesLeadershipAndCallbacks(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	n, err := New(testConfig(store, "drain", "n1", nil))
	require.NoError(t, err)
	started := make(chan LeaderLease, 1)
	stopped := make(chan LeaderLease, 1)
	n.OnStartedLeading(func(ctx context.Context, lease LeaderLease) error {
		started <- lease
		<-ctx.Done()
		return nil
	})
	n.OnStoppedLeading(func(lease LeaderLease) {
		stopped <- lease
	})

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

	waitLeader(t, n)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader callback did not start")
	}

	require.NoError(t, n.Drain(context.Background()))
	require.False(t, n.IsLeader())
	require.Equal(t, PhaseDraining, n.Phase())
	select {
	case lease := <-stopped:
		require.Equal(t, "n1", lease.HolderNodeID)
	case <-time.After(time.Second):
		t.Fatal("leader callback did not stop")
	}
	_, err = n.root.Get(context.Background(), "leader")
	require.ErrorIs(t, err, kv.ErrKeyNotFound)
}

func TestLeaderIsFencedByExternalLeaseChange(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	n, _, _ := startTestNode(t, testConfig(store, "fence", "n1", nil))
	waitLeader(t, n)

	now := time.Now()
	foreign := LeaderLease{
		ClusterID:       "fence",
		HolderNodeID:    "other",
		HolderSessionID: "foreign",
		Term:            99,
		Epoch:           99,
		FencingToken:    "foreign-token",
		AcquiredAt:      now,
		RenewedAt:       now,
		ExpiresAt:       now.Add(time.Second),
	}
	raw, err := marshalString(foreign)
	require.NoError(t, err)
	require.NoError(t, n.root.Put(context.Background(), "leader", raw, time.Second))

	require.Eventually(t, func() bool {
		return !n.IsLeader()
	}, time.Second, 20*time.Millisecond)
}

func TestConfigValidation(t *testing.T) {
	store, err := kv.NewMemory("")
	require.NoError(t, err)

	_, err = New(Config{})
	require.ErrorIs(t, err, ErrInvalidConfig)

	cfg := testConfig(store, "invalid", "bad/id", nil)
	_, err = New(cfg)
	require.ErrorIs(t, err, ErrInvalidConfig)

	cfg = testConfig(store, "bad/cluster", "n1", nil)
	_, err = New(cfg)
	require.ErrorIs(t, err, ErrInvalidConfig)

	cfg = testConfig(store, "invalid", "n1", nil)
	cfg.Mode = MemberMode("bad")
	_, err = New(cfg)
	require.ErrorIs(t, err, ErrInvalidConfig)

	cfg = testConfig(store, "invalid", "n1", nil)
	cfg.ElectionTimeoutMin = 2 * time.Second
	cfg.ElectionTimeoutMax = time.Second
	_, err = New(cfg)
	require.ErrorIs(t, err, ErrInvalidConfig)
}
