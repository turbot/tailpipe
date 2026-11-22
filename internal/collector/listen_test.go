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

// A cancel must not drop the events already waiting in the channel: the plugin has already saved its collection
// state for any chunk among them, so a dropped chunk is skipped (its rows lost) by the next collect (#508).
// The context is cancelled before the listener starts, so a select which picked the cancel at random would
// sometimes skip the buffered event - hence the repeats.
func TestListenToEvents_HandlesBufferedEventsOnCancel(t *testing.T) {
	for i := 0; i < 50; i++ {
		c := &Collector{Events: make(chan events.Event, 10), execution: &execution{}}
		c.Events <- events.NewStartedEvent("exec")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		done := make(chan struct{})
		go func() {
			c.listenToEvents(ctx)
			close(done)
		}()
		waitForReturn(t, done, "after the context was cancelled")

		if c.execution.state != ExecutionState_STARTED {
			t.Fatalf("iteration %d: the buffered event was dropped on cancel, state = %v", i, c.execution.state)
		}
	}
}

// Close waits for the listener to finish handing over the events it has, so that nothing is lost on cancel
// and nothing is still reading the collection temp dir when it is removed
func TestClose_WaitsForTheListener(t *testing.T) {
	dir := t.TempDir()
	c := &Collector{
		Events:            make(chan events.Event, 10),
		execution:         &execution{},
		collectionTempDir: dir + "/collection",
		sourcePath:        dir + "/collection/jsonl",
	}
	c.Events <- events.NewStartedEvent("exec")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.listener.Add(1)
	go func() {
		defer c.listener.Done()
		c.listenToEvents(ctx)
	}()

	c.Close()

	// no wait: once Close has returned the listener has finished with the event
	if c.execution.state != ExecutionState_STARTED {
		t.Fatalf("Close returned before the listener handled the buffered event, state = %v", c.execution.state)
	}
}
