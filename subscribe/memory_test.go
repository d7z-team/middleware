package subscribe

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemorySubscriberChannelBehaviors(t *testing.T) {
	testSubscriberChannelBehaviors(t, func(t *testing.T) CloserSubscriber {
		memory := NewMemorySubscriber()
		return &closerSubscriber{Subscriber: memory, closer: memory.Close}
	})
}

func TestEtcdSubscriberChannelBehaviors(t *testing.T) {
	testSubscriberChannelBehaviors(t, func(t *testing.T) CloserSubscriber {
		sub, err := NewSubscriberFromURL("etcd://127.0.0.1:2379?prefix=sub-test")
		if err != nil {
			t.Skipf("etcd unavailable: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := sub.Publish(ctx, "probe", "probe"); err != nil {
			_ = sub.Close()
			t.Skipf("etcd not reachable from test sandbox: %v", err)
		}
		return sub
	})
}

func testSubscriberChannelBehaviors(t *testing.T, factory func(t *testing.T) CloserSubscriber) {
	t.Helper()
	t.Run("context_cancel_closes_errors_channel", func(t *testing.T) {
		sub := factory(t)
		defer sub.Close()

		ctx, cancel := context.WithCancel(context.Background())
		stream, err := sub.Subscribe(ctx, "topic")
		require.NoError(t, err)

		cancel()

		select {
		case _, ok := <-stream.Events():
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("events channel did not close")
		}

		select {
		case _, ok := <-stream.Errors():
			require.False(t, ok)
		case <-time.After(time.Second):
			t.Fatal("errors channel did not close")
		}
	})

	t.Run("child_normalization", func(t *testing.T) {
		sub := factory(t)
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		child := sub.Child("", "/", "team", "/alpha/", "")
		stream, err := child.Subscribe(ctx, "topic")
		require.NoError(t, err)
		defer stream.Close()

		require.NoError(t, child.Publish(ctx, "topic", "payload"))

		select {
		case event := <-stream.Events():
			require.Contains(t, event.Key, "team/alpha/topic")
			require.Equal(t, "payload", event.Value)
		case <-ctx.Done():
			t.Fatal("timed out waiting for normalized child event")
		}
	})
}

func TestMemorySubscriberReportsOverflowOnErrorsChannel(t *testing.T) {
	sub := NewMemorySubscriber()
	defer sub.Close()

	stream, err := sub.Subscribe(context.Background(), "topic")
	require.NoError(t, err)
	defer stream.Close()

	for i := 0; i < subscriberEventBufferSize+1; i++ {
		require.NoError(t, sub.Publish(context.Background(), "topic", "value"))
	}

	select {
	case err := <-stream.Errors():
		require.ErrorIs(t, err, ErrSubscriptionOverflow)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for overflow error")
	}
}

func TestNewSubscriberFromURLInvalidCases(t *testing.T) {
	_, err := NewSubscriberFromURL(":%")
	require.Error(t, err)

	_, err = NewSubscriberFromURL("unknown://")
	require.Error(t, err)
}
