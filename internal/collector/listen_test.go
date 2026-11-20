package collector

import (
	"context"
	"testing"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/events"
)

func waitForReturn(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("listenToEvents did not return %s", what)
	}
}

// After a partition ends, Close closes the events channel; the listener must stop rather than spin on the
// closed channel (a receive on a closed channel never blocks)
func TestListenToEvents_StopsWhenEventsClosed(t *testing.T) {
	c := &Collector{Events: make(chan events.Event, 10)}
	done := make(chan struct{})
	// NOTE: the context is never cancelled - closing the channel alone must end the listener
	go func() {
		c.listenToEvents(context.Background())
		close(done)
	}()

	close(c.Events)
	waitForReturn(t, done, "after the events channel was closed")
}

// events already buffered when the channel is closed are still handled, then the listener stops
func TestListenToEvents_HandlesBufferedEventsThenStops(t *testing.T) {
	c := &Collector{Events: make(chan events.Event, 10), execution: &execution{}}
	c.Events <- events.NewStartedEvent("exec")
	close(c.Events)

	done := make(chan struct{})
	go func() {
		c.listenToEvents(context.Background())
		close(done)
	}()
	waitForReturn(t, done, "after draining the buffered events")

	if c.execution.state != ExecutionState_STARTED {
		t.Fatalf("buffered Started event was not handled, state = %v", c.execution.state)
	}
}

// the context still ends the listener while the channel is open
func TestListenToEvents_StopsOnContextCancel(t *testing.T) {
	c := &Collector{Events: make(chan events.Event, 10)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.listenToEvents(ctx)
		close(done)
	}()
	cancel()
	waitForReturn(t, done, "after the context was cancelled")
}
