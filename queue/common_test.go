package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/connects"
)

type namespaceFactory func(t *testing.T) CloserNamespace

func TestNewQueueFromURL(t *testing.T) {
	q, err := NewQueueFromURL("memory://")
	require.NoError(t, err)
	require.NoError(t, q.Close())

	q, err = NewQueueFromURL("mem://")
	require.NoError(t, err)
	require.NoError(t, q.Close())

	q, err = NewQueueFromURL("etcd://127.0.0.1:2379?prefix=test")
	require.NoError(t, err)
	require.NoError(t, q.Close())
}

func TestMemoryQueueContract(t *testing.T) {
	testQueueContract(t, func(t *testing.T) CloserNamespace {
		return NewMemoryQueueWithConfig(Config{
			DefaultAckTimeout: 30 * time.Millisecond,
			RequeueInterval:   5 * time.Millisecond,
			MaxDeliveries:     2,
		})
	})
}

func TestEtcdQueueContract(t *testing.T) {
	testQueueContract(t, newTestEtcdQueue)
}

func testQueueContract(t *testing.T, factory namespaceFactory) {
	t.Run("publish_consume_ack_and_state", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		producer := q.Producer()
		consumer := q.Consumer()
		admin := q.Admin()

		id, err := producer.Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)
		require.NotEmpty(t, id)

		state, err := admin.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusReady, state.Status)

		msg, err := consumer.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.Equal(t, 1, msg.Attempt)
		require.Equal(t, "payload", msg.Body)

		state, err = admin.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusInflight, state.Status)

		require.NoError(t, msg.Ack(ctx))

		state, err = admin.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusDone, state.Status)
	})

	t.Run("delay_dedup_and_batch", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		p := q.Producer()
		c := q.Consumer()

		start := time.Now()
		id1, err := p.Publish(ctx, "jobs", "one", &PublishOptions{Delay: 35 * time.Millisecond, DedupKey: "same"})
		require.NoError(t, err)
		id2, err := p.Publish(ctx, "jobs", "two", &PublishOptions{DedupKey: "same"})
		require.NoError(t, err)
		require.Equal(t, id1, id2)

		ids, err := p.PublishBatch(ctx, "jobs", []PublishRequest{
			{Body: "a"},
			{Body: "b", Opts: &PublishOptions{DedupKey: "dup"}},
			{Body: "c", Opts: &PublishOptions{DedupKey: "dup"}},
		})
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, ids[1], ids[2])

		msgA, err := c.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Contains(t, []MessageID{ids[0], ids[1]}, msgA.ID)
		require.NoError(t, msgA.Ack(ctx))

		msgB, err := c.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Contains(t, []MessageID{ids[0], ids[1]}, msgB.ID)
		require.NotEqual(t, msgA.ID, msgB.ID)
		require.NoError(t, msgB.Ack(ctx))

		msgDelayed, err := c.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id1, msgDelayed.ID)
		require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
		require.NoError(t, msgDelayed.Ack(ctx))
	})

	t.Run("stale_claim_is_rejected", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id, err := q.Producer().Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)

		msg1, err := q.Consumer().Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 20 * time.Millisecond})
		require.NoError(t, err)
		require.Equal(t, id, msg1.ID)

		time.Sleep(10 * time.Millisecond)
		require.NoError(t, msg1.Touch(ctx, 50*time.Millisecond))

		shortCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		none, err := q.Consumer().Consume(shortCtx, "jobs", nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, none)

		require.Eventually(t, func() bool {
			retryCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			msg2, err := q.Consumer().Consume(retryCtx, "jobs", nil)
			if err != nil {
				return false
			}
			defer msg2.Ack(context.Background())
			if msg2.ID != id || msg2.Attempt != 2 {
				return false
			}
			return errors.Is(msg1.Ack(ctx), ErrClaimMismatch)
		}, 2*time.Second, 15*time.Millisecond)
	})

	t.Run("cancel_delete_and_dead_letter", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		p := q.Producer()
		a := q.Admin()
		c := q.Consumer()

		readyID, err := p.Publish(ctx, "jobs", "ready", nil)
		require.NoError(t, err)
		deadID, err := p.Publish(ctx, "jobs", "dead", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)

		require.NoError(t, a.Cancel(ctx, "jobs", readyID))
		state, err := a.Get(ctx, "jobs", readyID)
		require.NoError(t, err)
		require.Equal(t, StatusCanceled, state.Status)

		msg, err := c.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
		require.NoError(t, err)
		require.Equal(t, deadID, msg.ID)

		require.Eventually(t, func() bool {
			count, err := a.CountDead(ctx, "jobs")
			return err == nil && count == 1
		}, 2*time.Second, 15*time.Millisecond)

		dead, err := a.GetDead(ctx, "jobs", deadID)
		require.NoError(t, err)
		require.Equal(t, "dead", dead.Body)

		page, err := a.ListDead(ctx, "jobs", &ListOptions{Limit: 10})
		require.NoError(t, err)
		require.Len(t, page.Messages, 1)

		require.NoError(t, a.RequeueDead(ctx, "jobs", deadID, 0))
		msg, err = c.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, deadID, msg.ID)
		require.NoError(t, msg.Ack(ctx))

		deleteID, err := p.Publish(ctx, "jobs", "delete", nil)
		require.NoError(t, err)
		require.NoError(t, a.Delete(ctx, "jobs", deleteID))
		state, err = a.Get(ctx, "jobs", deleteID)
		require.NoError(t, err)
		require.Equal(t, StatusMissing, state.Status)
	})

	t.Run("child_namespacing_and_parent_close", func(t *testing.T) {
		root := factory(t)
		defer root.Close()

		child := root.Child("team-a")
		id, err := child.Producer().Publish(t.Context(), "jobs", "payload", nil)
		require.NoError(t, err)

		msg, err := child.Consumer().Consume(t.Context(), "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.NoError(t, msg.Ack(t.Context()))

		state, err := root.Admin().Get(t.Context(), "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusMissing, state.Status)

		require.NoError(t, root.Close())
		_, err = child.Producer().Publish(t.Context(), "jobs", "after-close", nil)
		require.ErrorIs(t, err, ErrQueueClosed)
	})

	t.Run("child_close_does_not_close_parent", func(t *testing.T) {
		root := factory(t)
		defer root.Close()

		child := root.Child("child")
		childCloser, ok := child.(io.Closer)
		require.True(t, ok)
		require.NoError(t, childCloser.Close())

		_, err := child.Producer().Publish(t.Context(), "jobs", "payload", nil)
		require.ErrorIs(t, err, ErrQueueClosed)

		id, err := root.Producer().Publish(t.Context(), "jobs", "root", nil)
		require.NoError(t, err)
		msg, err := root.Consumer().Consume(t.Context(), "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.NoError(t, msg.Ack(t.Context()))
	})
}

func newTestEtcdQueue(t *testing.T) CloserNamespace {
	t.Helper()

	u, _ := url.Parse("etcd://127.0.0.1:2379")
	client, err := connects.NewEtcd(u)
	if err != nil {
		t.Skipf("etcd unavailable: %v", err)
	}

	prefix := fmt.Sprintf("queue-test-%d", time.Now().UnixNano())
	q := newEtcdQueue(client, prefix, client.Close, Config{
		DefaultAckTimeout: 30 * time.Millisecond,
		RequeueInterval:   5 * time.Millisecond,
		MaxDeliveries:     2,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := q.Producer().Publish(ctx, "probe", "probe", nil); err != nil {
		_ = q.Close()
		t.Skipf("etcd not reachable from test sandbox: %v", err)
	}
	return q
}
