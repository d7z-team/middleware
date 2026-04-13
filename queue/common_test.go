package queue

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.d7z.net/middleware/connects"
)

type queueFactory func(t *testing.T) CloserQueue

func TestNewQueueFromURL(t *testing.T) {
	q, err := NewQueueFromURL("memory://")
	require.NoError(t, err)
	require.NotNil(t, q)
	require.NoError(t, q.Close())

	q, err = NewQueueFromURL("mem://")
	require.NoError(t, err)
	require.NotNil(t, q)
	require.NoError(t, q.Close())

	q, err = NewQueueFromURL("etcd://127.0.0.1:2379?prefix=test")
	require.NoError(t, err)
	require.NotNil(t, q)
	require.NoError(t, q.Close())
}

func TestMemoryQueueContract(t *testing.T) {
	testQueueContract(t, "memory", func(t *testing.T) CloserQueue {
		return NewMemoryQueueWithConfig(Config{
			DefaultAckTimeout: 30 * time.Millisecond,
			RequeueInterval:   5 * time.Millisecond,
			MaxDeliveries:     3,
		})
	})
}

func TestEtcdQueueContract(t *testing.T) {
	testQueueContract(t, "etcd", newTestEtcdQueue)
}

func testQueueContract(t *testing.T, backend string, factory queueFactory) {
	t.Helper()

	t.Run(backend+"/publish_consume_ack", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id, err := q.Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)

		state, err := q.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusReady, state.Status)

		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.Equal(t, 1, msg.Attempt)

		state, err = q.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusInflight, state.Status)
		require.Equal(t, msg.Receipt, state.Receipt)

		require.NoError(t, msg.Ack(ctx))

		state, err = q.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusDone, state.Status)
	})

	t.Run(backend+"/publish_options_delay_and_dedup", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		start := time.Now()
		id1, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{
			Delay:    35 * time.Millisecond,
			DedupKey: "same",
		})
		require.NoError(t, err)

		id2, err := q.Publish(ctx, "jobs", "payload-2", &PublishOptions{DedupKey: "same"})
		require.NoError(t, err)
		require.Equal(t, id1, id2)

		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id1, msg.ID)
		require.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
		require.NoError(t, msg.Ack(ctx))
	})

	t.Run(backend+"/publish_batch", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		ids, err := q.PublishBatch(ctx, "jobs", []PublishRequest{
			{Body: "a"},
			{Body: "b", Opts: &PublishOptions{DedupKey: "dup"}},
			{Body: "c", Opts: &PublishOptions{DedupKey: "dup"}},
		})
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, ids[1], ids[2])

		msg1, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, "a", msg1.Body)
		require.NoError(t, msg1.Ack(ctx))

		msg2, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, "b", msg2.Body)
		require.NoError(t, msg2.Ack(ctx))
	})

	t.Run(backend+"/touch_requeue_and_receipt_mismatch", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id, err := q.Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)

		msg1, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 20 * time.Millisecond})
		require.NoError(t, err)

		time.Sleep(10 * time.Millisecond)
		require.NoError(t, msg1.Touch(ctx, 60*time.Millisecond))

		shortCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		next, err := q.Consume(shortCtx, "jobs", nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, next)

		require.Eventually(t, func() bool {
			retryCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()

			msg2, err := q.Consume(retryCtx, "jobs", nil)
			if err != nil {
				return false
			}
			defer msg2.Ack(context.Background())
			if msg2.ID != id || msg2.Receipt == msg1.Receipt || msg2.Attempt != 2 {
				return false
			}
			return errors.Is(msg1.Ack(ctx), ErrReceiptMismatch)
		}, 2*time.Second, 15*time.Millisecond)
	})

	t.Run(backend+"/nack_with_delay", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id, err := q.Publish(ctx, "jobs", "payload", nil)
		require.NoError(t, err)

		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)

		start := time.Now()
		require.NoError(t, msg.Nack(ctx, 30*time.Millisecond))

		msg, err = q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.Equal(t, 2, msg.Attempt)
		require.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond)
		require.NoError(t, msg.Ack(ctx))
	})

	t.Run(backend+"/get_peek_count", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id1, err := q.Publish(ctx, "jobs", "one", nil)
		require.NoError(t, err)
		id2, err := q.Publish(ctx, "jobs", "two", &PublishOptions{Delay: 50 * time.Millisecond})
		require.NoError(t, err)

		peek, err := q.Peek(ctx, "jobs", 2)
		require.NoError(t, err)
		require.Len(t, peek, 2)
		require.Equal(t, id1, peek[0].ID)
		require.Equal(t, id2, peek[1].ID)

		stats, err := q.Count(ctx, "jobs")
		require.NoError(t, err)
		require.EqualValues(t, 2, stats.Ready)
		require.EqualValues(t, 0, stats.Inflight)

		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)

		stats, err = q.Count(ctx, "jobs")
		require.NoError(t, err)
		require.EqualValues(t, 1, stats.Ready)
		require.EqualValues(t, 1, stats.Inflight)

		state, err := q.Get(ctx, "jobs", msg.ID)
		require.NoError(t, err)
		require.Equal(t, StatusInflight, state.Status)

		missing, err := q.Get(ctx, "jobs", "missing")
		require.NoError(t, err)
		require.Equal(t, StatusMissing, missing.Status)

		require.NoError(t, msg.Ack(ctx))
	})

	t.Run(backend+"/dead_letter_lifecycle", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		id1, err := q.Publish(ctx, "jobs", "dead-1", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)
		id2, err := q.Publish(ctx, "jobs", "dead-2", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)

		_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
		require.NoError(t, err)
		_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			count, err := q.CountDead(ctx, "jobs")
			return err == nil && count == 2
		}, 2*time.Second, 15*time.Millisecond)

		deadOne, err := q.GetDead(ctx, "jobs", id1)
		require.NoError(t, err)
		require.Equal(t, "dead-1", deadOne.Body)

		page1, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 1})
		require.NoError(t, err)
		require.Len(t, page1.Messages, 1)
		require.True(t, page1.HasMore)

		page2, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 1, Cursor: page1.NextCursor})
		require.NoError(t, err)
		require.Len(t, page2.Messages, 1)

		require.NoError(t, q.RequeueDead(ctx, "jobs", id1, 20*time.Millisecond))
		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id1, msg.ID)
		require.NoError(t, msg.Ack(ctx))

		n, err := q.RequeueAllDead(ctx, "jobs", 10, 0)
		require.NoError(t, err)
		require.Equal(t, 1, n)

		msg, err = q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id2, msg.ID)
		require.NoError(t, msg.Ack(ctx))

		deadCount, err := q.CountDead(ctx, "jobs")
		require.NoError(t, err)
		require.EqualValues(t, 0, deadCount)
	})

	t.Run(backend+"/delete_and_cancel", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		idDead, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 1})
		require.NoError(t, err)

		msg, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
		require.NoError(t, err)
		require.ErrorIs(t, q.Delete(ctx, "jobs", msg.ID), ErrInvalidState)

		require.Eventually(t, func() bool {
			count, err := q.CountDead(ctx, "jobs")
			return err == nil && count == 1
		}, 2*time.Second, 15*time.Millisecond)

		require.NoError(t, q.DeleteDead(ctx, "jobs", idDead))
		_, err = q.GetDead(ctx, "jobs", idDead)
		require.ErrorIs(t, err, ErrDeadNotFound)

		idReady, err := q.Publish(ctx, "jobs", "ready", nil)
		require.NoError(t, err)
		require.NoError(t, q.Cancel(ctx, "jobs", idReady))

		state, err := q.Get(ctx, "jobs", idReady)
		require.NoError(t, err)
		require.Equal(t, StatusCanceled, state.Status)

		idInflight, err := q.Publish(ctx, "jobs", "inflight", nil)
		require.NoError(t, err)
		inflightMsg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, idInflight, inflightMsg.ID)

		require.NoError(t, q.Cancel(ctx, "jobs", idInflight))
		canceled, err := inflightMsg.CancelRequested(ctx)
		require.NoError(t, err)
		require.True(t, canceled)
		require.NoError(t, inflightMsg.Ack(ctx))

		idDelete, err := q.Publish(ctx, "jobs", "delete-me", nil)
		require.NoError(t, err)
		require.NoError(t, q.Delete(ctx, "jobs", idDelete))

		deletedState, err := q.Get(ctx, "jobs", idDelete)
		require.NoError(t, err)
		require.Equal(t, StatusMissing, deletedState.Status)
	})

	t.Run(backend+"/child_isolation", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		childA := q.Child("a")
		childB := q.Child("b")
		ctx := t.Context()

		id, err := childA.Publish(ctx, "jobs", "payload-a", nil)
		require.NoError(t, err)

		msg, err := childA.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.NoError(t, msg.Ack(ctx))

		state, err := childB.Get(ctx, "jobs", id)
		require.NoError(t, err)
		require.Equal(t, StatusMissing, state.Status)
	})

	t.Run(backend+"/concurrent_consumption", func(t *testing.T) {
		q := factory(t)
		defer q.Close()

		ctx := t.Context()
		const total = 20
		for i := 0; i < total; i++ {
			_, err := q.Publish(ctx, "jobs", fmt.Sprintf("payload-%02d", i), nil)
			require.NoError(t, err)
		}

		seen := make(map[string]int)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					consumeCtx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
					msg, err := q.Consume(consumeCtx, "jobs", nil)
					cancel()
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							return
						}
						require.NoError(t, err)
					}
					require.NoError(t, msg.Ack(context.Background()))
					mu.Lock()
					seen[msg.ID]++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		require.Len(t, seen, total)
		for _, count := range seen {
			require.Equal(t, 1, count)
		}
	})

	t.Run(backend+"/validation_and_close", func(t *testing.T) {
		q := factory(t)

		_, err := q.Publish(t.Context(), "", "payload", nil)
		require.ErrorIs(t, err, ErrInvalidTopic)
		_, err = q.Peek(t.Context(), "jobs", 0)
		require.ErrorIs(t, err, ErrInvalidLimit)
		_, err = q.Consume(t.Context(), "jobs", &ConsumeOptions{AckTimeout: -time.Second})
		require.ErrorIs(t, err, ErrInvalidAckTimeout)
		_, err = q.Publish(t.Context(), "jobs", "payload", &PublishOptions{Delay: -time.Second})
		require.ErrorIs(t, err, ErrInvalidDelay)

		require.NoError(t, q.Close())

		_, err = q.Publish(t.Context(), "jobs", "payload", nil)
		require.ErrorIs(t, err, ErrQueueClosed)
		_, err = q.Consume(t.Context(), "jobs", nil)
		require.ErrorIs(t, err, ErrQueueClosed)
		_, err = q.Count(t.Context(), "jobs")
		require.ErrorIs(t, err, ErrQueueClosed)
		require.ErrorIs(t, q.Cancel(t.Context(), "jobs", "id"), ErrQueueClosed)

		msg := &Message{}
		require.ErrorIs(t, msg.Ack(t.Context()), ErrInvalidMessage)
		require.ErrorIs(t, msg.Nack(t.Context(), 0), ErrInvalidMessage)
		require.ErrorIs(t, msg.Touch(t.Context(), time.Second), ErrInvalidMessage)
		_, err = msg.CancelRequested(t.Context())
		require.ErrorIs(t, err, ErrInvalidMessage)
	})
}

func TestMemoryQueueParentCloseClosesChildren(t *testing.T) {
	root := NewMemoryQueue()
	child := root.Child("child")

	require.NoError(t, root.Close())

	_, err := child.Publish(t.Context(), "jobs", "payload", nil)
	require.ErrorIs(t, err, ErrQueueClosed)
}

func TestMemoryQueueChildCloseDoesNotCloseParent(t *testing.T) {
	root := NewMemoryQueue()
	defer root.Close()

	child := root.Child("child")
	require.NoError(t, child.(CloserQueue).Close())

	_, err := child.Publish(t.Context(), "jobs", "payload", nil)
	require.ErrorIs(t, err, ErrQueueClosed)

	id, err := root.Publish(t.Context(), "jobs", "root-payload", nil)
	require.NoError(t, err)
	msg, err := root.Consume(t.Context(), "jobs", nil)
	require.NoError(t, err)
	require.Equal(t, id, msg.ID)
	require.NoError(t, msg.Ack(t.Context()))
	assert.Equal(t, StatusDone, mustGetState(t, root, "jobs", id).Status)
}

func TestEtcdQueueConcurrentConsumeSingleDelivery(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	ctx := t.Context()
	id, err := q.Publish(ctx, "jobs", "payload", nil)
	require.NoError(t, err)

	const workers = 8
	type result struct {
		id  string
		err error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			msg, err := q.Consume(consumeCtx, "jobs", nil)
			if err == nil {
				_ = msg.Ack(context.Background())
				results <- result{id: msg.ID}
				return
			}
			results <- result{err: err}
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	for result := range results {
		if result.err == nil {
			require.Equal(t, id, result.id)
			successes++
			continue
		}
		require.ErrorIs(t, result.err, context.DeadlineExceeded)
	}
	require.Equal(t, 1, successes)
}

func TestEtcdQueueConcurrentAckContention(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	ctx := t.Context()
	id, err := q.Publish(ctx, "jobs", "payload", nil)
	require.NoError(t, err)

	msg, err := q.Consume(ctx, "jobs", nil)
	require.NoError(t, err)
	require.Equal(t, id, msg.ID)

	const workers = 6
	var successCount atomic.Int32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := msg.Ack(context.Background())
			if err == nil {
				successCount.Add(1)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	require.EqualValues(t, 1, successCount.Load())
	for err := range errs {
		if err == nil {
			continue
		}
		require.ErrorIs(t, err, ErrReceiptMismatch)
	}
}

func TestEtcdQueueConcurrentRequeueDeadContention(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	ctx := t.Context()
	id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 1})
	require.NoError(t, err)

	_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		count, err := q.CountDead(ctx, "jobs")
		return err == nil && count == 1
	}, 2*time.Second, 15*time.Millisecond)

	const workers = 6
	var successCount atomic.Int32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := q.RequeueDead(context.Background(), "jobs", id, 0)
			if err == nil {
				successCount.Add(1)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	require.EqualValues(t, 1, successCount.Load())
	for err := range errs {
		if err == nil {
			continue
		}
		require.ErrorIs(t, err, ErrDeadNotFound)
	}

	msg, err := q.Consume(ctx, "jobs", nil)
	require.NoError(t, err)
	require.Equal(t, id, msg.ID)
	require.NoError(t, msg.Ack(ctx))
}

func TestEtcdQueueSoakMultiProducerMultiConsumer(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	const (
		producers         = 4
		consumers         = 6
		messagesPerWorker = 40
		totalMessages     = producers * messagesPerWorker
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	type produced struct {
		id   string
		body string
	}

	producedCh := make(chan produced, totalMessages)
	var producersWG sync.WaitGroup
	for p := 0; p < producers; p++ {
		producerID := p
		producersWG.Add(1)
		go func() {
			defer producersWG.Done()
			for i := 0; i < messagesPerWorker; i++ {
				body := fmt.Sprintf("p%d-%03d", producerID, i)
				id, err := q.Publish(ctx, "jobs", body, nil)
				require.NoError(t, err)
				producedCh <- produced{id: id, body: body}
			}
		}()
	}
	producersWG.Wait()
	close(producedCh)

	expected := make(map[string]string, totalMessages)
	for item := range producedCh {
		expected[item.id] = item.body
	}

	seen := make(map[string]int, totalMessages)
	var seenMu sync.Mutex
	var acked atomic.Int32
	var consumersWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumersWG.Add(1)
		go func() {
			defer consumersWG.Done()
			for {
				if int(acked.Load()) >= totalMessages {
					return
				}
				consumeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err := q.Consume(consumeCtx, "jobs", &ConsumeOptions{AckTimeout: 250 * time.Millisecond})
				cancel()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
						if int(acked.Load()) >= totalMessages {
							return
						}
						continue
					}
					require.NoError(t, err)
				}
				require.Equal(t, expected[msg.ID], msg.Body)
				require.NoError(t, msg.Ack(context.Background()))
				seenMu.Lock()
				seen[msg.ID]++
				seenMu.Unlock()
				acked.Add(1)
			}
		}()
	}
	consumersWG.Wait()

	require.EqualValues(t, totalMessages, acked.Load())
	require.Len(t, seen, totalMessages)
	for id, count := range seen {
		require.Equalf(t, 1, count, "message %s consumed unexpected number of times", id)
	}

	stats, err := q.Count(t.Context(), "jobs")
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.Ready)
	require.EqualValues(t, 0, stats.Inflight)
	require.EqualValues(t, 0, stats.Dead)
}

func TestQueueIdempotencyAndRetryBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 30 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
					MaxDeliveries:     2,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name+"/ack_retry_boundary", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", nil)
			require.NoError(t, err)

			msg, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 20 * time.Millisecond})
			require.NoError(t, err)
			require.Equal(t, id, msg.ID)
			require.NoError(t, msg.Ack(ctx))
			require.ErrorIs(t, msg.Ack(ctx), ErrReceiptMismatch)
		})

		t.Run(tt.name+"/cancel_idempotency_boundary", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", nil)
			require.NoError(t, err)

			require.NoError(t, q.Cancel(ctx, "jobs", id))
			require.ErrorIs(t, q.Cancel(ctx, "jobs", id), ErrInvalidState)
		})

		t.Run(tt.name+"/delete_idempotency_boundary", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", nil)
			require.NoError(t, err)

			require.NoError(t, q.Delete(ctx, "jobs", id))
			require.ErrorIs(t, q.Delete(ctx, "jobs", id), ErrMessageNotFound)
		})

		t.Run(tt.name+"/delete_dead_idempotency_boundary", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 1})
			require.NoError(t, err)

			_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				count, err := q.CountDead(ctx, "jobs")
				return err == nil && count == 1
			}, 2*time.Second, 15*time.Millisecond)

			require.NoError(t, q.DeleteDead(ctx, "jobs", id))
			require.ErrorIs(t, q.DeleteDead(ctx, "jobs", id), ErrDeadNotFound)
		})

		t.Run(tt.name+"/requeue_dead_idempotency_boundary", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 1})
			require.NoError(t, err)

			_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				count, err := q.CountDead(ctx, "jobs")
				return err == nil && count == 1
			}, 2*time.Second, 15*time.Millisecond)

			require.NoError(t, q.RequeueDead(ctx, "jobs", id, 0))
			require.ErrorIs(t, q.RequeueDead(ctx, "jobs", id, 0), ErrDeadNotFound)

			msg, err := q.Consume(ctx, "jobs", nil)
			require.NoError(t, err)
			require.Equal(t, id, msg.ID)
			require.NoError(t, msg.Ack(ctx))
		})
	}
}

func TestQueueInterruptedConsumerRecovery(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 20 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
					MaxDeliveries:     2,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name+"/unacked_message_requeues_after_consumer_interrupt", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", nil)
			require.NoError(t, err)

			msg1, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 20 * time.Millisecond})
			require.NoError(t, err)
			require.Equal(t, id, msg1.ID)

			require.Eventually(t, func() bool {
				retryCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
				defer cancel()
				msg2, err := q.Consume(retryCtx, "jobs", nil)
				if err != nil {
					return false
				}
				defer msg2.Ack(context.Background())
				return msg2.ID == id && msg2.Attempt == 2 && msg2.Receipt != msg1.Receipt
			}, 2*time.Second, 15*time.Millisecond)
		})

		t.Run(tt.name+"/repeated_interrupts_move_message_to_dead_letter", func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 2})
			require.NoError(t, err)

			_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				msg2, err := q.Consume(context.Background(), "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
				if err != nil {
					return false
				}
				return msg2.ID == id && msg2.Attempt == 2
			}, 2*time.Second, 15*time.Millisecond)

			require.Eventually(t, func() bool {
				count, err := q.CountDead(ctx, "jobs")
				return err == nil && count == 1
			}, 2*time.Second, 15*time.Millisecond)

			dead, err := q.GetDead(ctx, "jobs", id)
			require.NoError(t, err)
			require.Equal(t, 2, dead.Attempt)
		})
	}
}

func TestQueueConcurrentDedupKey(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 30 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			const workers = 10
			ids := make(chan string, workers)
			errs := make(chan error, workers)
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{DedupKey: "same"})
					if err != nil {
						errs <- err
						return
					}
					ids <- id
				}()
			}
			wg.Wait()
			close(ids)
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}

			unique := make(map[string]struct{})
			for id := range ids {
				unique[id] = struct{}{}
			}
			require.Len(t, unique, 1)

			stats, err := q.Count(ctx, "jobs")
			require.NoError(t, err)
			require.EqualValues(t, 1, stats.Ready)

			msg, err := q.Consume(ctx, "jobs", nil)
			require.NoError(t, err)
			require.NoError(t, msg.Ack(ctx))
		})
	}
}

func TestQueueRequeueAllDeadLimitBoundary(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 20 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			var ids []string
			for i := 0; i < 3; i++ {
				id, err := q.Publish(ctx, "jobs", fmt.Sprintf("payload-%d", i), &PublishOptions{MaxDeliveries: 1})
				require.NoError(t, err)
				ids = append(ids, id)
			}

			for range ids {
				_, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
				require.NoError(t, err)
			}

			require.Eventually(t, func() bool {
				count, err := q.CountDead(ctx, "jobs")
				return err == nil && count == 3
			}, 2*time.Second, 15*time.Millisecond)

			n, err := q.RequeueAllDead(ctx, "jobs", 1, 0)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			stats, err := q.Count(ctx, "jobs")
			require.NoError(t, err)
			require.EqualValues(t, 1, stats.Ready)
			require.EqualValues(t, 2, stats.Dead)

			msg, err := q.Consume(ctx, "jobs", nil)
			require.NoError(t, err)
			require.NoError(t, msg.Ack(ctx))

			deadCount, err := q.CountDead(ctx, "jobs")
			require.NoError(t, err)
			require.EqualValues(t, 2, deadCount)
		})
	}
}

func TestQueueListDeadCursorBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 20 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			for i := 0; i < 3; i++ {
				_, err := q.Publish(ctx, "jobs", fmt.Sprintf("payload-%d", i), &PublishOptions{MaxDeliveries: 1})
				require.NoError(t, err)
			}
			for i := 0; i < 3; i++ {
				_, err := q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
				require.NoError(t, err)
			}

			require.Eventually(t, func() bool {
				count, err := q.CountDead(ctx, "jobs")
				return err == nil && count == 3
			}, 2*time.Second, 15*time.Millisecond)

			page1, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 2})
			require.NoError(t, err)
			require.Len(t, page1.Messages, 2)
			require.True(t, page1.HasMore)
			require.NotEmpty(t, page1.NextCursor)

			page2, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 2, Cursor: page1.NextCursor})
			require.NoError(t, err)
			require.Len(t, page2.Messages, 1)
			require.False(t, page2.HasMore)
			require.Empty(t, page2.NextCursor)

			page3, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 2, Cursor: "999"})
			require.NoError(t, err)
			require.Len(t, page3.Messages, 2)
			require.True(t, page3.HasMore)

			page4, err := q.ListDead(ctx, "jobs", &ListOptions{Limit: 2, Cursor: "invalid"})
			require.NoError(t, err)
			require.Len(t, page4.Messages, 2)
			require.True(t, page4.HasMore)
		})
	}
}

func TestQueueCancelReadyPreventsDelivery(t *testing.T) {
	for _, tt := range []struct {
		name    string
		factory queueFactory
	}{
		{
			name: "memory",
			factory: func(t *testing.T) CloserQueue {
				return NewMemoryQueueWithConfig(Config{
					DefaultAckTimeout: 30 * time.Millisecond,
					RequeueInterval:   5 * time.Millisecond,
				})
			},
		},
		{
			name:    "etcd",
			factory: newTestEtcdQueue,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.factory(t)
			defer q.Close()

			ctx := t.Context()
			id, err := q.Publish(ctx, "jobs", "payload", nil)
			require.NoError(t, err)

			require.NoError(t, q.Cancel(ctx, "jobs", id))

			state, err := q.Get(ctx, "jobs", id)
			require.NoError(t, err)
			require.Equal(t, StatusCanceled, state.Status)

			consumeCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			msg, err := q.Consume(consumeCtx, "jobs", nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, msg)
		})
	}
}

func TestEtcdQueueCrossRaceCancelVsAck(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	ctx := t.Context()
	id, err := q.Publish(ctx, "jobs", "payload", nil)
	require.NoError(t, err)

	msg, err := q.Consume(ctx, "jobs", nil)
	require.NoError(t, err)
	require.Equal(t, id, msg.ID)

	const workers = 2
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	go func() {
		defer wg.Done()
		errs <- q.Cancel(context.Background(), "jobs", id)
	}()
	go func() {
		defer wg.Done()
		errs <- msg.Ack(context.Background())
	}()
	wg.Wait()
	close(errs)

	var ackOK, cancelOK bool
	for err := range errs {
		if err == nil {
			if !cancelOK {
				cancelOK = true
				continue
			}
			ackOK = true
			continue
		}
		require.True(t, errors.Is(err, ErrReceiptMismatch) || errors.Is(err, ErrInvalidState))
	}

	state, err := q.Get(ctx, "jobs", id)
	require.NoError(t, err)
	require.Contains(t, []MessageStatus{StatusDone, StatusInflight}, state.Status)
	if state.Status == StatusInflight {
		canceled, err := msg.CancelRequested(ctx)
		require.NoError(t, err)
		require.True(t, canceled)
		require.NoError(t, msg.Ack(ctx))
	}
	_ = ackOK
	_ = cancelOK
}

func TestEtcdQueueCrossRaceDeleteVsRequeueDead(t *testing.T) {
	q := newTestEtcdQueue(t)
	defer q.Close()

	ctx := t.Context()
	id, err := q.Publish(ctx, "jobs", "payload", &PublishOptions{MaxDeliveries: 1})
	require.NoError(t, err)

	_, err = q.Consume(ctx, "jobs", &ConsumeOptions{AckTimeout: 15 * time.Millisecond})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		count, err := q.CountDead(ctx, "jobs")
		return err == nil && count == 1
	}, 2*time.Second, 15*time.Millisecond)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs <- q.DeleteDead(context.Background(), "jobs", id)
	}()
	go func() {
		defer wg.Done()
		errs <- q.RequeueDead(context.Background(), "jobs", id, 0)
	}()
	wg.Wait()
	close(errs)

	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		require.True(t, errors.Is(err, ErrDeadNotFound) || errors.Is(err, ErrMessageNotFound))
	}
	require.Equal(t, 1, successes)

	state, err := q.Get(ctx, "jobs", id)
	require.NoError(t, err)
	switch state.Status {
	case StatusMissing:
	case StatusReady:
		msg, err := q.Consume(ctx, "jobs", nil)
		require.NoError(t, err)
		require.Equal(t, id, msg.ID)
		require.NoError(t, msg.Ack(ctx))
	default:
		t.Fatalf("unexpected message status after race: %s", state.Status)
	}
}

func mustGetState(t *testing.T, q Queue, topic, id string) *MessageState {
	t.Helper()
	state, err := q.Get(t.Context(), topic, id)
	require.NoError(t, err)
	return state
}

func newTestEtcdQueue(t *testing.T) CloserQueue {
	t.Helper()

	u, _ := url.Parse("etcd://127.0.0.1:2379")
	client, err := connects.NewEtcd(u)
	if err != nil {
		t.Skipf("Etcd not available: %v", err)
	}

	prefix := fmt.Sprintf("queue-test-%d", time.Now().UnixNano())
	return newEtcdQueue(client, prefix, client.Close, Config{})
}
