package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClaimedMessageRejectsInvalidCapabilities(t *testing.T) {
	var msg ClaimedMessage
	ctx := context.Background()

	require.ErrorIs(t, msg.Ack(ctx), ErrInvalidMessage)
	require.ErrorIs(t, msg.Nack(ctx, 0), ErrInvalidMessage)
	require.ErrorIs(t, msg.Touch(ctx, time.Second), ErrInvalidMessage)
	canceled, err := msg.CancelRequested(ctx)
	require.ErrorIs(t, err, ErrInvalidMessage)
	require.False(t, canceled)
}

func TestMemoryQueueExtendedAdminBehaviors(t *testing.T) {
	testQueueExtendedAdminBehaviors(t, func(t *testing.T) CloserNamespace {
		return NewMemoryQueueWithConfig(Config{
			DefaultAckTimeout: 25 * time.Millisecond,
			RequeueInterval:   5 * time.Millisecond,
			MaxDeliveries:     2,
		})
	})
}

func TestEtcdQueueExtendedAdminBehaviors(t *testing.T) {
	testQueueExtendedAdminBehaviors(t, newTestEtcdQueue)
}

func testQueueExtendedAdminBehaviors(t *testing.T, factory namespaceFactory) {
	t.Helper()
	t.Run("peek_count_and_delete_inflight", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		producer := q.Producer()
		consumer := q.Consumer()
		admin := q.Admin()

		firstID, err := producer.Publish(ctx, "jobs", "first", nil)
		require.NoError(t, err)
		secondID, err := producer.Publish(ctx, "jobs", "second", nil)
		require.NoError(t, err)

		peek, err := admin.Peek(ctx, "jobs", 2)
		require.NoError(t, err)
		require.Len(t, peek, 2)
		require.Equal(t, firstID, peek[0].ID)
		require.Equal(t, secondID, peek[1].ID)
		require.ErrorIs(t, admin.Delete(ctx, "jobs", "missing"), ErrMessageNotFound)

		stats, err := admin.Count(ctx, "jobs")
		require.NoError(t, err)
		require.Equal(t, &Stats{Ready: 2, Total: 2}, stats)

		_, err = admin.Peek(ctx, "jobs", 0)
		require.ErrorIs(t, err, ErrInvalidLimit)

		msg, err := consumer.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, firstID, msg.ID)

		stats, err = admin.Count(ctx, "jobs")
		require.NoError(t, err)
		require.Equal(t, &Stats{Ready: 1, Inflight: 1, Total: 2}, stats)

		err = admin.Delete(ctx, "jobs", msg.ID)
		require.ErrorIs(t, err, ErrInvalidState)
		require.NoError(t, msg.Ack(ctx))
	})

	t.Run("cancel_requested_and_nack", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id, err := q.Producer().Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)

		msg, err := q.Consumer().Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.Equal(t, 1, msg.Attempt)

		cancelRequested, err := msg.CancelRequested(ctx)
		require.NoError(t, err)
		require.False(t, cancelRequested)

		require.NoError(t, q.Admin().Cancel(ctx, "jobs", id))

		cancelRequested, err = msg.CancelRequested(ctx)
		require.NoError(t, err)
		require.True(t, cancelRequested)

		require.ErrorIs(t, msg.Nack(ctx, -time.Millisecond), ErrInvalidDelay)
		require.NoError(t, msg.Nack(ctx, 15*time.Millisecond))

		stats, err := q.Admin().Count(ctx, "jobs")
		require.NoError(t, err)
		require.Equal(t, &Stats{Ready: 1, Total: 1}, stats)

		require.Eventually(t, func() bool {
			retryCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			retry, err := q.Consumer().Consume(retryCtx, "jobs", nil)
			if err != nil {
				return false
			}
			defer retry.Ack(context.Background())
			return retry.ID == id && retry.Attempt == 2
		}, time.Second, 10*time.Millisecond)

		err = q.Admin().Cancel(ctx, "jobs", id)
		require.ErrorIs(t, err, ErrInvalidState)
	})

	t.Run("requeue_all_dead_and_delete_dead", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		admin := q.Admin()
		consumer := q.Consumer()

		firstID, err := q.Producer().Publish(ctx, "jobs", "first-dead", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)
		secondID, err := q.Producer().Publish(ctx, "jobs", "second-dead", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)

		firstMsg, err := consumer.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 10 * time.Millisecond})
		require.NoError(t, err)
		require.Equal(t, firstID, firstMsg.ID)
		secondMsg, err := consumer.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 10 * time.Millisecond})
		require.NoError(t, err)
		require.Equal(t, secondID, secondMsg.ID)

		require.Eventually(t, func() bool {
			count, err := admin.CountDead(ctx, "jobs")
			return err == nil && count == 2
		}, 2*time.Second, 15*time.Millisecond)

		requeued, err := admin.RequeueAllDead(ctx, "jobs", 1, 0)
		require.NoError(t, err)
		require.Equal(t, 1, requeued)

		stats, err := admin.Count(ctx, "jobs")
		require.NoError(t, err)
		require.EqualValues(t, 1, stats.Ready)
		require.EqualValues(t, 1, stats.Dead)
		require.EqualValues(t, 2, stats.Total)

		dead, err := admin.ListDead(ctx, "jobs", &ListOptions{Limit: 10})
		require.NoError(t, err)
		require.Len(t, dead.Messages, 1)
		require.NoError(t, admin.DeleteDead(ctx, "jobs", dead.Messages[0].ID))

		countDead, err := admin.CountDead(ctx, "jobs")
		require.NoError(t, err)
		require.Zero(t, countDead)

		msg, err := consumer.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Contains(t, []MessageID{firstID, secondID}, msg.ID)
		require.NoError(t, msg.Ack(ctx))

		err = admin.DeleteDead(ctx, "jobs", dead.Messages[0].ID)
		require.ErrorIs(t, err, ErrDeadNotFound)
	})
}
