package subscribe

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewSubscriberFromURL(t *testing.T) {
	sub, err := NewSubscriberFromURL("memory://")
	require.NoError(t, err)
	require.NoError(t, sub.Close())

	sub, err = NewSubscriberFromURL("mem://")
	require.NoError(t, err)
	require.NoError(t, sub.Close())

	sub, err = NewSubscriberFromURL("etcd://127.0.0.1:2379?prefix=sub")
	require.NoError(t, err)
	require.NoError(t, sub.Close())
}

func TestMemorySubscriberContract(t *testing.T) {
	testSubscriberContract(t, func(t *testing.T) CloserSubscriber {
		memory := NewMemorySubscriber()
		return &closerSubscriber{Subscriber: memory, closer: memory.Close}
	})
}

func TestEtcdSubscriberContract(t *testing.T) {
	testSubscriberContract(t, func(t *testing.T) CloserSubscriber {
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

func testSubscriberContract(t *testing.T, factory func(t *testing.T) CloserSubscriber) {
	t.Run("publish_subscribe", func(t *testing.T) {
		sub := factory(t)
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stream, err := sub.Subscribe(ctx, "topic")
		require.NoError(t, err)
		defer stream.Close()

		require.NoError(t, sub.Publish(ctx, "topic", "hello"))

		select {
		case event := <-stream.Events():
			require.Equal(t, "hello", event.Value)
			require.NotEmpty(t, event.Key)
		case <-ctx.Done():
			t.Fatal("timed out waiting for event")
		}
	})

	t.Run("child_namespace_isolated", func(t *testing.T) {
		sub := factory(t)
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		child := sub.Child("child")
		stream, err := child.Subscribe(ctx, "topic")
		require.NoError(t, err)
		defer stream.Close()

		require.NoError(t, child.Publish(ctx, "topic", "from-child"))

		select {
		case event := <-stream.Events():
			require.Equal(t, "from-child", event.Value)
			require.Contains(t, event.Key, "child/topic")
		case <-ctx.Done():
			t.Fatal("timed out waiting for child event")
		}
	})

	t.Run("multiple_subscribers_receive_same_event", func(t *testing.T) {
		sub := factory(t)
		defer sub.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		s1, err := sub.Subscribe(ctx, "broadcast")
		require.NoError(t, err)
		defer s1.Close()
		s2, err := sub.Subscribe(ctx, "broadcast")
		require.NoError(t, err)
		defer s2.Close()

		require.NoError(t, sub.Publish(ctx, "broadcast", "value"))

		for _, stream := range []Subscription{s1, s2} {
			select {
			case event := <-stream.Events():
				require.Equal(t, "value", event.Value)
			case <-ctx.Done():
				t.Fatal("timed out waiting for broadcast")
			}
		}
	})

	t.Run("close_and_context_cancel_close_channels", func(t *testing.T) {
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
			t.Fatal("subscription events channel did not close")
		}
	})
}

func TestParentCloseInvalidatesMemoryChild(t *testing.T) {
	parent := NewMemorySubscriber()
	child, ok := parent.Child("child").(*MemorySubscriber)
	require.True(t, ok)

	stream, err := child.Subscribe(context.Background(), "topic")
	require.NoError(t, err)

	require.NoError(t, parent.Close())

	select {
	case _, ok := <-stream.Events():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("child subscription was not closed")
	}

	closedStream, err := child.Subscribe(context.Background(), "topic-2")
	require.NoError(t, err)
	select {
	case _, ok := <-closedStream.Events():
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("subscribe after parent close did not return closed stream")
	}
}
