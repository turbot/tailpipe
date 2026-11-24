package collector

import (
	"context"
	"errors"
	"testing"
	"time"
)

// done may be called more than once for an execution (a cancel ends the wait, then the listener reports a late
// plugin error): the later calls must return rather than block, or Close, which waits for the listener, hangs
// after Ctrl+C.
func TestExecutionDone_NeverBlocks(t *testing.T) {
	e := &execution{completionChan: make(chan error, 1)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// the cancelled wait records the cancel as the result...
	if err := e.waitForCompletion(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForCompletion = %v, want context canceled", err)
	}

	// ...and a late report from the listener must not block
	finished := make(chan struct{})
	go func() {
		e.done(errors.New("plugin error"))
		e.done(nil)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("execution.done blocked on a second call")
	}

	if e.state != ExecutionState_ERROR {
		t.Fatalf("state = %v, want error", e.state)
	}
}

// the first result is the one the waiter gets
func TestExecutionDone_WaiterGetsFirstResult(t *testing.T) {
	e := &execution{completionChan: make(chan error, 1)}
	first := errors.New("first")
	e.done(first)
	e.done(errors.New("second"))

	if err := e.waitForCompletion(context.Background()); err != first {
		t.Fatalf("waitForCompletion = %v, want the first result", err)
	}
}
