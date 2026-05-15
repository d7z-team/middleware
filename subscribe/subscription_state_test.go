package subscribe

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscriptionStateOverflow(t *testing.T) {
	state := newSubscriptionState()
	for i := 0; i < subscriberEventBufferSize+1; i++ {
		require.NoError(t, state.trySendEvent(context.Background(), Event{Value: "value"}))
	}

	select {
	case err := <-state.Errors():
		require.ErrorIs(t, err, ErrSubscriptionOverflow)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for overflow signal")
	}

	state.closeChannels()
}

func TestSubscriptionStateCloseWhileSending(t *testing.T) {
	state := newSubscriptionState()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5000; j++ {
				_ = state.trySendEvent(context.Background(), Event{Value: "value"})
				state.trySendError(errors.New("boom"))
			}
		}()
	}

	state.closeChannels()
	wg.Wait()
}
