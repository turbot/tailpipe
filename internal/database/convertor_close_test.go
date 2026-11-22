package database

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// closeTestConverter is a Converter, like newTestChunkConverter, which records every chunk the processor
// converts (in batches) and can hold the processor at the start of its first batch.
type closeTestConverter struct {
	*Converter
	mu        sync.Mutex
	converted []int32
	// closed once the processor is blocked at the start of its first batch
	inBatch chan struct{}
	release chan struct{}
	once    sync.Once
}

func newCloseTestConverter(t *testing.T, hold bool) *closeTestConverter {
	t.Helper()
	c := &closeTestConverter{
		Converter: newTestChunkConverter(t),
		inBatch:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	c.onBatchStart = func(chunks []int32) {
		c.mu.Lock()
		c.converted = append(c.converted, chunks...)
		c.mu.Unlock()
		if hold {
			c.once.Do(func() {
				close(c.inBatch)
				<-c.release
			})
		}
	}
	t.Cleanup(c.releaseProcessor)
	return c
}

func (c *closeTestConverter) releaseProcessor() {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
}

func (c *closeTestConverter) convertedChunks() []int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := append([]int32(nil), c.converted...)
	sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
	return res
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func closeInBackground(w *Converter) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()
	return done
}

// A collect which is cancelled (or ends) while chunks are waiting must not lose them: the plugin has already saved
// its collection state past each chunk it sent, so a chunk dropped here is skipped, and its rows lost, by the next
// collect. Close therefore converts every chunk accepted, and only then returns, so the files can be removed after it
// (removing them earlier is what made a cancelled collect report "file does not exist", #508).
func TestConverter_CloseConvertsAcceptedChunksBeforeReturning(t *testing.T) {
	c := newCloseTestConverter(t, true)

	if err := c.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, c.inBatch, "the processor to start its first batch")
	// these queue up behind the batch in progress
	for chunk := int32(2); chunk <= 4; chunk++ {
		if err := c.AddChunk("exec", chunk); err != nil {
			t.Fatal(err)
		}
	}

	closed := closeInBackground(c.Converter)
	select {
	case <-closed:
		t.Fatal("Close returned while chunks were still waiting to be converted")
	case <-time.After(200 * time.Millisecond):
	}

	c.releaseProcessor()
	waitFor(t, closed, "Close to return once every chunk was converted")

	if got := c.convertedChunks(); len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("converted chunks = %v, want 1 to 4", got)
	}
	if c.hasScheduledChunks() {
		t.Fatal("chunks left scheduled after Close")
	}
}

// a conversion failure while closing is still reported as usual: Close waits for the report to be made
func TestConverter_CloseStillReportsConversionErrors(t *testing.T) {
	w := newTestChunkConverter(t)
	var mu sync.Mutex
	var reported int
	w.statusFunc = func(_ int64, _ int64, errs ...error) {
		mu.Lock()
		reported += len(errs)
		mu.Unlock()
	}

	// the chunk file does not exist, so the conversion fails
	if err := w.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}
	w.Close()

	mu.Lock()
	defer mu.Unlock()
	if reported != 1 {
		t.Fatalf("%d errors reported for the failed chunk, want 1", reported)
	}
}

// A chunk which arrives after Close is dropped: it is not scheduled, no processor is started for it, and it leaves
// nothing for anyone to wait for.
func TestConverter_ChunkAfterCloseIsDropped(t *testing.T) {
	c := newCloseTestConverter(t, false)
	c.Close()

	for chunk := int32(0); chunk < 5; chunk++ {
		if err := c.AddChunk("exec", chunk); err != nil {
			t.Fatalf("AddChunk after Close failed: %v", err)
		}
	}
	if c.hasScheduledChunks() {
		t.Fatal("a chunk was scheduled after Close")
	}
	if got := c.convertedChunks(); len(got) != 0 {
		t.Fatalf("chunks %v were converted after Close", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitForConversions(ctx); err != nil {
		t.Fatalf("a dropped chunk left something to wait for: %v", err)
	}
}

// Close on a converter which never received a chunk (a collect cancelled before the first one) returns at once,
// and so does a second Close.
func TestConverter_CloseWithNoChunksAndTwice(t *testing.T) {
	w := newTestChunkConverter(t)
	waitFor(t, closeInBackground(w), "Close with no chunks")
	waitFor(t, closeInBackground(w), "a second Close")
}

// Chunks added concurrently with Close are each either converted or dropped, never half-scheduled: Close returns,
// and the wait group is never left unbalanced (which would panic, or hang a later wait).
func TestConverter_CloseWhileChunksAreBeingAdded(t *testing.T) {
	for i := 0; i < 50; i++ {
		w := newTestChunkConverter(t)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for a := int32(0); a < 4; a++ {
			wg.Add(1)
			go func(a int32) {
				defer wg.Done()
				<-start
				for chunk := int32(0); chunk < 20; chunk++ {
					if err := w.AddChunk("exec", a*20+chunk); err != nil {
						t.Errorf("AddChunk failed: %v", err)
					}
				}
			}(a)
		}
		close(start)
		waitFor(t, closeInBackground(w), "Close while chunks were being added")
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.WaitForConversions(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: chunks left unconverted: %v", i, err)
		}
	}
}
