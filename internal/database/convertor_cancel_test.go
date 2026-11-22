package database

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newCancellableTestConverter returns a Converter, like newTestChunkConverter, with a collection context which the
// test can cancel (directly, or through Close). reported counts the calls the converter makes to report errors.
func newCancellableTestConverter(t *testing.T) (w *Converter, reported *atomic.Int32) {
	t.Helper()
	reported = &atomic.Int32{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w = &Converter{
		executionId: "exec",
		sourceDir:   t.TempDir(),
		ctx:         ctx,
		cancel:      cancel,
		statusFunc: func(_ int64, _ int64, errs ...error) {
			if len(errs) > 0 {
				reported.Add(1)
			}
		},
	}
	// skip schema inference - there is no database
	w.viewQueryOnce.Do(func() {})
	return w, reported
}

// blockFirstBatch makes the chunk processor stop at the start of its first batch until the returned release func
// is called. It returns a channel which is closed once the processor is blocked there.
func blockFirstBatch(w *Converter) (inBatch <-chan struct{}, release func()) {
	entered := make(chan struct{})
	releaseCh := make(chan struct{})
	var once sync.Once
	w.onBatchStart = func() {
		once.Do(func() {
			close(entered)
			<-releaseCh
		})
	}
	var releaseOnce sync.Once
	return entered, func() { releaseOnce.Do(func() { close(releaseCh) }) }
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// A collect cancelled while chunks are waiting to be converted must stop quietly: the chunk files may be removed
// at any moment, so a failed conversion after the cancel is not an error to show the user (the "file does not
// exist" / "No files found" errors of #508), and the waiters are still released.
func TestConverter_CancelledCollectReportsNoErrors(t *testing.T) {
	w, reported := newCancellableTestConverter(t)
	inBatch, release := blockFirstBatch(w)
	defer release()

	// the chunk file does not exist, as when the temp dir has been removed under the converter
	if err := w.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, inBatch, "the processor to start its first batch")

	// more chunks queue up behind the first batch, then Ctrl+C
	if err := w.AddChunk("exec", 2); err != nil {
		t.Fatal(err)
	}
	w.cancel()
	release()

	// every scheduled chunk is released, so nothing waits forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitDone := make(chan error, 1)
	go func() { waitDone <- w.wgWait(ctx) }()
	if err := <-waitDone; err != nil {
		t.Fatalf("scheduled chunks were not released after the cancel: %v", err)
	}

	w.Close() // waits for the processor, so every report it would make has been made
	if n := reported.Load(); n != 0 {
		t.Fatalf("%d errors were reported for a cancelled collect, want none", n)
	}
}

// wgWait waits for every scheduled chunk to be released, ignoring the collection context (which is cancelled)
func (w *Converter) wgWait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// the unhappy side: a conversion failure on a collect which is NOT cancelled is still reported
func TestConverter_FailureWithoutCancelIsStillReported(t *testing.T) {
	w, reported := newCancellableTestConverter(t)

	if err := w.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WaitForConversions(ctx); err != nil {
		t.Fatal(err)
	}
	// WaitForConversions returns once the batch is released, which is just before the error is reported
	// (the report is made after processChunks returns) - Close waits for the processor to finish
	w.Close()
	if n := reported.Load(); n != 1 {
		t.Fatalf("%d errors reported for a missing chunk file, want 1", n)
	}
}

// Close must not return while a batch is being converted: the collector removes the chunk files after it.
func TestConverter_CloseWaitsForBatchInProgress(t *testing.T) {
	w, _ := newCancellableTestConverter(t)
	inBatch, release := blockFirstBatch(w)
	defer release()

	if err := w.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, inBatch, "the processor to start its batch")

	closed := make(chan struct{})
	go func() {
		w.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a batch was still being processed")
	case <-time.After(200 * time.Millisecond):
	}

	release()
	waitFor(t, closed, "Close to return once the batch finished")

	if !w.cancelled() {
		t.Fatal("Close did not cancel the collection context")
	}
}

// A chunk which arrives after the converter was closed (the plugin is still sending) is dropped: it is not
// scheduled, no processor is started for it, and it reports nothing.
func TestConverter_ChunkAfterCloseIsDropped(t *testing.T) {
	w, reported := newCancellableTestConverter(t)
	w.Close()

	for c := int32(0); c < 5; c++ {
		if err := w.AddChunk("exec", c); err != nil {
			t.Fatalf("AddChunk after Close failed: %v", err)
		}
	}
	if w.hasScheduledChunks() {
		t.Fatal("a chunk was scheduled after Close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.wgWait(ctx); err != nil {
		t.Fatalf("a dropped chunk left the wait group unbalanced: %v", err)
	}
	if n := reported.Load(); n != 0 {
		t.Fatalf("%d errors reported after Close, want none", n)
	}
}

// Close on a converter which never received a chunk (a collect cancelled before the first one) returns at once,
// and so does a second Close.
func TestConverter_CloseWithNoChunks(t *testing.T) {
	w, _ := newCancellableTestConverter(t)
	closed := make(chan struct{})
	go func() {
		w.Close()
		close(closed)
	}()
	waitFor(t, closed, "Close with no chunks")

	// a second Close (the collector's Close after a test's, say) must not wait for a lock that is never released
	again := make(chan struct{})
	go func() {
		w.Close()
		close(again)
	}()
	waitFor(t, again, "a second Close")
}
