package parquet

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/viper"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
)

// TODO HACK
const defaultParquetWorkerCount = 1 // 5
const chunkBufferLength = 1000
const defaultWorkerMemoryMb = 4096
const minWorkerMemoryMb = 4096

// Converter struct executes all the conversions for a single collection
// it therefore has a unique execution id, and will potentially convert of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunk number
type Converter struct {
	// the execution id
	id string

	// the file chunks numbers available to process
	chunks      []int32
	chunkLock   sync.Mutex
	chunkSignal *sync.Cond
	// the channel to send execution to the workers
	jobChan chan *parquetJob
	// waitGroup to track job completion
	wg sync.WaitGroup
	// the cancel function for the context used to manage the job
	cancel context.CancelFunc

	// the number of chunks processed so far
	completionCount int32
	// the number of rows written
	rowCount int64
	// the number of rows which were NOT converted due to conversion errors encountered
	failedRowCount int64

	// the source file location
	sourceDir string
	// the dest file location
	destDir string
	// helper to provide unique file roots
	fileRootProvider *FileRootProvider

	// the format string for the conversion query will be the same for all chunks - build once and store
	viewQueryFormat string

	// the table conversionSchema - populated when the first chunk arrives if the conversionSchema is not already complete
	conversionSchema *schema.ConversionSchema
	// the source schema - used to build the conversionSchema
	tableSchema *schema.TableSchema

	// viewQueryOnce ensures the schema inference only happens once for the first chunk,
	// even if multiple chunks arrive concurrently. Combined with schemaWg, this ensures
	// all subsequent chunks wait for the initial schema inference to complete before proceeding.
	viewQueryOnce sync.Once
	// schemaWg is used to block processing of subsequent chunks until the initial
	// schema inference is complete. This ensures all chunks wait for the schema
	// to be fully initialized before proceeding with their processing.
	schemaWg sync.WaitGroup

	// the partition being collected
	Partition *config.Partition
	// func which we call with updated row count
	statusFunc func(int64, int64, ...error)
}

func NewParquetConverter(ctx context.Context, cancel context.CancelFunc, executionId string, partition *config.Partition, sourceDir string, tableSchema *schema.TableSchema, statusFunc func(int64, int64, ...error)) (*Converter, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	// normalise the table schema to use lowercase column names
	tableSchema.NormaliseColumnTypes()

	w := &Converter{
		id:               executionId,
		chunks:           make([]int32, 0, chunkBufferLength), // Pre-allocate reasonable capacity
		Partition:        partition,
		cancel:           cancel,
		sourceDir:        sourceDir,
		destDir:          destDir,
		tableSchema:      tableSchema,
		statusFunc:       statusFunc,
		fileRootProvider: &FileRootProvider{},
	}
	// create the condition variable using the same lock
	w.chunkSignal = sync.NewCond(&w.chunkLock)

	// initialise the workers
	if err := w.createWorkers(ctx); err != nil {
		return nil, fmt.Errorf("failed to create workers: %w", err)
	}
	// start the goroutine to schedule the jobs
	go w.scheduler(ctx)

	// done
	return w, nil
}

func (w *Converter) Close() {
	slog.Info("closing Converter")
	// close the close channel to signal to the job schedulers to exit
	w.cancel()
}

// AddChunk adds a new chunk to the list of chunks to be processed
// if this is the first chunk, determine if we have a full conversionSchema yet and if not infer from the chunk
// signal the scheduler that `chunks are available
func (w *Converter) AddChunk(executionId string, chunk int32) error {
	var err error
	w.schemaWg.Wait()

	// Execute schema inference exactly once for the first chunk.
	// The WaitGroup ensures all subsequent chunks wait for this to complete.
	// If schema inference fails, the error is captured and returned to the caller.
	w.viewQueryOnce.Do(func() {
		w.schemaWg.Add(1)
		defer w.schemaWg.Done()
		if err = w.buildConversionSchema(executionId, chunk); err != nil {
			// err will be returned by the parent function
			return
		}
		w.viewQueryFormat = buildViewQuery(w.conversionSchema)
	})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}
	w.chunkLock.Lock()
	w.chunks = append(w.chunks, chunk)
	w.chunkLock.Unlock()

	w.wg.Add(1)

	// Signal that new chunk is available
	// Using Signal instead of Broadcast as only one worker needs to wake up
	w.chunkSignal.Signal()

	return nil
}

// WaitForConversions waits for all jobs to be processed or for the context to be cancelled
func (w *Converter) WaitForConversions(ctx context.Context) {
	slog.Info("Converter.WaitForConversions - waiting for all jobs to be processed or context to be cancelled.")
	// wait for the wait group within a goroutine so we can also check the context
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		slog.Info("WaitForConversions - context cancelled.")
	case <-done:
		slog.Info("WaitForConversions - all jobs processed.")
	}
}

// waitForSignal waits for the condition signal or context cancellation
// returns true if context was cancelled
func (w *Converter) waitForSignal(ctx context.Context) bool {
	w.chunkLock.Lock()
	defer w.chunkLock.Unlock()

	select {
	case <-ctx.Done():
		return true
	default:
		w.chunkSignal.Wait()
		return false
	}
}

// the scheduler is responsible for sending jobs to the workere
// it listens for signals on the chunkWrittenSignal channel and enqueues jobs when they arrive
func (w *Converter) scheduler(ctx context.Context) {
	defer close(w.jobChan)

	for {
		chunk, ok := w.getNextChunk()
		if !ok {
			if w.waitForSignal(ctx) {
				slog.Debug("scheduler shutting down due to context cancellation")
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case w.jobChan <- &parquetJob{chunkNumber: chunk}:
		}
	}
}

func (w *Converter) getNextChunk() (int32, bool) {
	w.chunkLock.Lock()
	defer w.chunkLock.Unlock()

	if len(w.chunks) == 0 {
		return 0, false
	}

	// Take from end - more efficient as it avoids shifting elements
	lastIdx := len(w.chunks) - 1
	chunk := w.chunks[lastIdx]
	w.chunks = w.chunks[:lastIdx]
	return chunk, true
}

func (w *Converter) addJobErrors(errorList ...error) {
	var failedRowCount int64

	for _, err := range errorList {
		var conversionError = &ConversionError{}
		if errors.As(err, &conversionError) {
			failedRowCount = atomic.AddInt64(&w.failedRowCount, conversionError.RowsAffected)
		}
		slog.Error("conversion error", "error", err)
	}

	// update the status function with the new error count (no need to use atomic for errorList as we are already locked)
	w.statusFunc(atomic.LoadInt64(&w.rowCount), failedRowCount, errorList...)
}

// updateRowCount atomically increments the row count and calls the statusFunc
func (w *Converter) updateRowCount(count int64) {
	atomic.AddInt64(&w.rowCount, count)
	// call the status function with the new row count
	w.statusFunc(atomic.LoadInt64(&w.rowCount), atomic.LoadInt64(&w.failedRowCount))
}

// createWorkers initializes and starts parquet conversion workers based on configured memory limits
// It calculates the optimal number of workers and memory allocation per worker using the following logic:
// - If no memory limit is set, uses defaultParquetWorkerCount workers with defaultWorkerMemoryMb per worker
// - If memory limit is set, ensures each worker gets at least minWorkerMemoryMb, reducing worker count if needed
// - Reserves memory for the main process by dividing total memory by (workerCount + 1)
// - Creates and starts the calculated number of workers, each with their allocated memory
// Returns error if worker creation fails
func (w *Converter) createWorkers(ctx context.Context) error {
	// determine the number of workers to start
	// see if there was a memory limit
	maxMemoryMb := viper.GetInt(pconstants.ArgMemoryMaxMb)

	// if no memory limit is set, calculate based on default worker count and min memory per worker
	if maxMemoryMb == 0 {
		maxMemoryMb = defaultParquetWorkerCount * defaultWorkerMemoryMb
	}

	// calculate memory per worker and adjust worker count if needed
	// - reserve memory for main process by dividing maxMemory by (workerCount + 1)
	// - if calculated memory per worker is less than minimum required:
	//   - reduce worker count to ensure each worker has minimum required memory
	//   - ensure at least 1 worker remains
	memoryPerWorkerMb := maxMemoryMb / defaultParquetWorkerCount
	workerCount := defaultParquetWorkerCount
	if memoryPerWorkerMb < minWorkerMemoryMb {
		// reduce worker count to ensure minimum memory per worker
		workerCount = maxMemoryMb / minWorkerMemoryMb
		if workerCount < 1 {
			workerCount = 1
		}
		memoryPerWorkerMb = maxMemoryMb / workerCount
		if memoryPerWorkerMb < minWorkerMemoryMb {
			return fmt.Errorf("not enough memory available for workers - require at least %d for a single worker", 2*minWorkerMemoryMb)
		}
	}

	// create the job channel
	w.jobChan = make(chan *parquetJob, workerCount*2)

	// start the workers
	for i := 0; i < workerCount; i++ {
		wk, err := newConversionWorker(w, memoryPerWorkerMb)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)
		}
		// start the worker
		go wk.start(ctx)
	}
	return nil
}
