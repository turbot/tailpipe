package parquet

import (
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"log/slog"
	"sync"
	"time"
)

type parquetJobError struct {
	executionId string
	err         error
}

// the parquetJobPool struct represents all the conversions that need to be done for a single 'parquetJobPool'
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type parquetJobPool struct {
	id string
	// the file chunks numbers available to process
	chunks    []int
	chunkLock sync.RWMutex
	// the index into 'chunks' of the next chunk number to process
	nextChunkIndex int
	// the number of chunks processed so far
	completionCount int32
	// the number of rows written
	rowCount int64

	// The channel to send execution to the workers
	jobChan chan parquetJob
	// The channel to receive errors from the workers
	errorChan chan parquetJobError

	// channel to indicate we are closing
	closing chan struct{}

	// the source file location
	sourceDir string
	// the dest file location
	destDir string

	// the number of workers
	workerCount int
	// helper to provide unique file roots
	fileRootProvider *FileRootProvider

	// sync.Cond to wait for the next chunkNumber to be available
	chunkWrittenSignal *sync.Cond

	// error which occurred during execution
	errors     []error
	errorsLock sync.RWMutex
	// channel to mark parquetJobPool completion
	done chan struct{}

	Partition            *config.Partition
	SchemaFunc           func() *schema.RowSchema
	UpdateActiveDuration func(duration time.Duration)
}

// Start the parquetJobPool - spawn workers
func (w *parquetJobPool) Start() error {
	slog.Info("starting parquet Writer", "worker count", w.workerCount)
	// start a goroutine to read the error channel
	go w.readJobErrors()
	// start the workers
	for i := 0; i < w.workerCount; i++ {
		wk, err := newParquetConversionWorker(w.jobChan, w.errorChan, w.sourceDir, w.destDir, w.fileRootProvider)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start()

	}

	return nil
}

func newParquetJobPool(id string,
	partition *config.Partition,
	schemaFunc func() *schema.RowSchema,
	updateActiveDuration func(duration time.Duration),
	workers int, sourceDir, destDir string,
) *parquetJobPool {
	return &parquetJobPool{
		id:                   id,
		chunkWrittenSignal:   sync.NewCond(&sync.Mutex{}),
		done:                 make(chan struct{}),
		Partition:            partition,
		SchemaFunc:           schemaFunc,
		UpdateActiveDuration: updateActiveDuration,
		jobChan:              make(chan parquetJob, workers*2),
		errorChan:            make(chan parquetJobError),
		closing:              make(chan struct{}),
		workerCount:          workers,
		sourceDir:            sourceDir,
		destDir:              destDir,

		fileRootProvider: &FileRootProvider{},
	}
}

// StartExecution schedules a parquetJobPool to be processed
// a job group represents a set of linked jobs, e.w. a set of JSONL files that need to be converted to Parquet for
// a given collection execution
func (w *parquetJobPool) StartExecution(executionId string, partition *config.Partition, schemeFunc func() *schema.RowSchema, updateActiveDuration func(increment time.Duration)) error {
	slog.Debug("parquetJobPool.StartExecution", "execution id", executionId)
	// start a thread to schedule
	// this will terminate when the parquetJobPool is complete as the parquetJobPool.done channel will be closed
	go w.scheduler()
	return nil
}

func (w *parquetJobPool) AddChunk(executionId string, chunks ...int) error {
	// add the chunks to the parquetJobPool
	w.chunkLock.Lock()
	w.chunks = append(w.chunks, chunks...)
	w.chunkLock.Unlock()

	// signal the scheduler that there are new chunks
	w.chunkWrittenSignal.L.Lock()
	w.chunkWrittenSignal.Broadcast()
	w.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *parquetJobPool) GetChunksWritten(id string) (int32, error) {
	// if the job has errors, terminate
	w.errorsLock.RLock()
	defer w.errorsLock.RUnlock()
	if len(w.errors) > 0 {
		err := errors.Join(w.errors...)
		return -1, fmt.Errorf("job group %s has errors: %w", id, err)
	}
	return w.completionCount, nil
}

func (w *parquetJobPool) GetRowCount() (int64, error) {
	return w.rowCount, nil
}

func (w *parquetJobPool) JobGroupComplete(id string) error {
	// TODO compbine with CLOSE????
	slog.Info("parquetJobPool - parquetJobPool complete", "execution id", id)
	// close the done channel to signal the scheduler to exit
	close(w.done)
	return nil
}

func (w *parquetJobPool) Close() {
	slog.Info("closing parquetJobPool", "job pool", w)
	// close the close channel to signal to the job schedulers to exit
	close(w.closing)
	// close the error channel to terminate the error reader
	close(w.errorChan)
	// do not close the job channel - the workers will terminate when `closing` is closed
}

// scheduleJob is schedules a parquetJobPool to be processed
func (w *parquetJobPool) scheduler() {
	for {
		// try to write to the parquetJobPool channel
		// if we can't, wait for a parquetJobPool to be processed

		// build the filename we assume the filename is <execution_id>_<chunkNumber>.jsonl
		// this will wait until there is a chunkNumber available to process
		// if the parquetJobPool is complete, it will return -1
		nextChunk := w.waitForNextChunk()
		// if no nextChunk returned, either the writer is closing or the parquetJobPool is complete
		if nextChunk == -1 {
			slog.Debug("exiting scheduler", "execution id", w.id)
			return
		}

		// send the parquetJobPool to the workers
		// do in a goroutine so we can also check for completion/closure
		j := parquetJob{
			groupId:              w.id,
			chunkNumber:          nextChunk,
			completionCount:      &w.completionCount,
			rowCount:             &w.rowCount,
			Partition:            w.Partition,
			SchemaFunc:           w.SchemaFunc,
			UpdateActiveDuration: w.UpdateActiveDuration,
		}
		// TODO #conversion is this costly to do thousands of times?
		sendChan := make(chan struct{})
		go func() {
			w.jobChan <- j
			close(sendChan)
		}()

		select {
		// wait for send completion
		case <-sendChan:
			//slog.Debug("sent parquetJobPool to worker", "chunk", j.chunkNumber, "completion count", *j.completionCount)
			// so we sent a parquetJobPool
			// update the next chunkNumber
			w.nextChunkIndex++

		// is Writer closing?
		case <-w.closing:
			slog.Debug("write is closing - exiting scheduler", "execution id", w.id)
			return
			// Note we do not check <-parquetJobPool.done as the parquetJobPool cannot be done before all chunks are processed
			// and if the parquetJobPool was complete we would have returned -1 from waitForNextChunk
		}
	}
}

func (w *parquetJobPool) waitForNextChunk() int {
	// if we have chunks available, build a filename from the next chunkNumber
	if w.nextChunkIndex < len(w.chunks) {
		return w.chunks[w.nextChunkIndex]
	}

	// so there are no chunks available
	// wait for chunkWrittenSignal to be signalled

	// do in a goroutine so we can also check for completion/closure
	var chunkChan = make(chan struct{})
	go func() {
		// wait for chunkWrittenSignal to be signalled
		w.chunkWrittenSignal.L.Lock()
		w.chunkWrittenSignal.Wait()
		w.chunkWrittenSignal.L.Unlock()
		close(chunkChan)
	}()

	select {
	case <-chunkChan:
		// get chunkNumber
		w.chunkLock.RLock()
		defer w.chunkLock.RUnlock()

		// NOTE: trim the buffer
		w.chunks = w.chunks[w.nextChunkIndex:]
		// update the index to point to the start of the trimmed buffer
		w.nextChunkIndex = 0

		if len(w.chunks) == 0 {
			slog.Warn("no more chunks - parquetJobPool is done", "execution id", w.id)
			// no more chunks - parquetJobPool is done
			return -1
		}
		// we have new chunks - return the next
		return w.chunks[w.nextChunkIndex]

	case <-w.closing:
		// Writer is closing
		return -1
	case <-w.done:
		// parquetJobPool is done
		return -1
	}
}

func (w *parquetJobPool) readJobErrors() {
	for err := range w.errorChan {
		w.errorsLock.Lock()
		w.errors = append(w.errors, err.err)
		w.errorsLock.Unlock()
	}
}
