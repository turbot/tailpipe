package database

import (
	"context"
	"sync"
	"testing"
	"time"
)

// newTestChunkConverter returns a Converter that can schedule and "process" chunks without a database:
// the chunk files do not exist, so each batch ends in a conversion error, which still goes through the whole
// schedule/process/wait-group path that these tests are about
func newTestChunkConverter(t *testing.T) *Converter {
	t.Helper()
	w := &Converter{
		executionId: "exec",
		sourceDir:   t.TempDir(),
		statusFunc:  func(int64, int64, ...error) {},
	}
	// skip schema inference - there is no database
	w.viewQueryOnce.Do(func() {})
	return w
}

// Many chunks added concurrently while processors start and finish must all be converted, so that
// WaitForConversions returns, and the wait group must never go negative (it panics if it does).
// The exact lost-chunk timing is forced by TestConverter_ChunkAddedAsProcessorFindsQueueEmpty.
func TestConverter_LastChunkAddedWhileProcessorFinishing(t *testing.T) {
	const (
		iterations = 100
		adders     = 4
		perAdder   = 25
	)
	for i := 0; i < iterations; i++ {
		w := newTestChunkConverter(t)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for a := 0; a < adders; a++ {
			wg.Add(1)
			go func(a int) {
				defer wg.Done()
				<-start
				for c := 0; c < perAdder; c++ {
					if err := w.AddChunk("exec", int32(a*perAdder+c)); err != nil {
						t.Errorf("AddChunk failed: %v", err)
					}
				}
			}(a)
		}
		close(start)
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.WaitForConversions(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d: chunks were never converted: %v", i, err)
		}
		if w.hasScheduledChunks() {
			t.Fatalf("iteration %d: chunks left scheduled after WaitForConversions", i)
		}
	}
}

// the unhappy side: a chunk added after an earlier batch has finished (processor idle) is picked up too, and
// a chunk that fails to convert still releases the wait
func TestConverter_ChunkAddedAfterIdle(t *testing.T) {
	w := newTestChunkConverter(t)
	for c := int32(0); c < 3; c++ {
		if err := w.AddChunk("exec", c); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := w.WaitForConversions(ctx)
		cancel()
		if err != nil {
			t.Fatalf("chunk %d not processed: %v", c, err)
		}
	}
}

// Forces the exact timing of the bug: a chunk is added after the processor has found its queue empty but
// before it has released the process lock, so AddChunk cannot start a processor of its own.
func TestConverter_ChunkAddedAsProcessorFindsQueueEmpty(t *testing.T) {
	for i := 0; i < 100; i++ {
		chunkAddedAsProcessorFindsQueueEmpty(t, i)
	}
}

func chunkAddedAsProcessorFindsQueueEmpty(t *testing.T, iteration int) {
	t.Helper()
	w := newTestChunkConverter(t)

	added := make(chan struct{})
	var once sync.Once
	w.onQueueEmpty = func() {
		once.Do(func() {
			// the processor holds the process lock here, so this AddChunk can only queue the chunk
			if err := w.AddChunk("exec", 2); err != nil {
				t.Errorf("AddChunk failed: %v", err)
			}
			close(added)
		})
	}

	if err := w.AddChunk("exec", 1); err != nil {
		t.Fatal(err)
	}

	// like the collector, only wait once every chunk has been added
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatalf("iteration %d: processor never reached the end of its queue", iteration)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.WaitForConversions(ctx); err != nil {
		t.Fatalf("iteration %d: the last chunk was never converted: %v", iteration, err)
	}
	if w.hasScheduledChunks() {
		t.Fatalf("iteration %d: chunks left scheduled after WaitForConversions", iteration)
	}
}
