package parquet

import (
	"fmt"
	"log/slog"
	"sync"
)

type jobGroupError struct {
	jobGroupId string
	err        error
}

// fileJobPool[T] is a pool of workers that process file jobs
type fileJobPool[T any] struct {
	// The queue of jobGroups to be processed
	// this is a map keyed by execution id and the value is the list of json files to be processed
	jobGroups map[string]*jobGroup[T]
	// The lock to protect the jobGroups map
	jobGroupLock sync.RWMutex

	// The channel to send jobGroups to the workers
	jobChan chan fileJob[T]
	// The channel to receive errors from the workers
	errorChan chan jobGroupError

	// channel to indicate we are closing
	closing chan struct{}

	// the source file location
	sourceDir string
	// the dest file location
	destDir string

	// the number of workers
	workerCount int
	// function to create a new worker
	newWorker newWorkerFunc[T]
	// helper to provide unique file roots
	fileRootProvider *FileRootProvider
}

// create a new fileJobPool[T]
func newFileJobPool[T any](workers int, sourceDir, destDir string, newWorker newWorkerFunc[T]) *fileJobPool[T] {
	return &fileJobPool[T]{
		// map of running job groups, keyed by id
		jobGroups: make(map[string]*jobGroup[T]),
		// create worker jobGroup channel
		// amount of buffering is not crucial as the Writer will also be buffering the jobGroups
		// just set to twice number of workers
		jobChan:          make(chan fileJob[T], workers*2),
		errorChan:        make(chan jobGroupError),
		closing:          make(chan struct{}),
		workerCount:      workers,
		sourceDir:        sourceDir,
		destDir:          destDir,
		newWorker:        newWorker,
		fileRootProvider: &FileRootProvider{},
	}
}

// Start the fileJobPool[T] - spawn workers
func (w *fileJobPool[T]) Start() error {
	slog.Info("starting parquet Writer", "worker count", w.workerCount)
	// start a goroutine to read the error channel
	go w.readJobErrors()
	// start the workers
	for i := 0; i < w.workerCount; i++ {
		wk, err := w.newWorker(w.jobChan, w.errorChan, w.sourceDir, w.destDir, w.fileRootProvider)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start()

	}

	return nil
}

// StartJobGroup schedules a jobGroup to be processed
// a job group represents a set of linked jobs, e.g. a set of JSONL files that need to be converted to Parquet for
// a given collection execution
func (w *fileJobPool[T]) StartJobGroup(id string, payload T) error {
	slog.Debug("fileJobPool[T].StartJobGroup", "execution id", id)
	// we expect this execution id WIL NOT be in the map already
	// if it is, we should return an error
	if _, ok := w.jobGroups[id]; ok {
		return fmt.Errorf("job group id %s already exists", id)
	}

	jobGroup := newJobGroup(id, payload)
	w.jobGroupLock.Lock()
	// add the jobGroup to the map
	w.jobGroups[id] = jobGroup
	w.jobGroupLock.Unlock()

	// start a thread to schedule
	// this will terminate when the jobGroup is complete as the jobGroup.done channel will be closed
	go w.scheduler(jobGroup)
	return nil
}

func (w *fileJobPool[T]) AddChunk(id string, chunks ...int) error {
	// get the jobGroup
	w.jobGroupLock.RLock()
	job, ok := w.jobGroups[id]
	w.jobGroupLock.RUnlock()

	if !ok {
		return fmt.Errorf("group id %s does not exist", id)
	}

	// add the chunks to the jobGroup
	job.chunkLock.Lock()
	job.chunks = append(job.chunks, chunks...)
	job.chunkLock.Unlock()

	// signal the scheduler that there are new chunks
	job.chunkWrittenSignal.L.Lock()
	job.chunkWrittenSignal.Broadcast()
	job.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *fileJobPool[T]) GetChunksWritten(id string) (int32, error) {
	w.jobGroupLock.RLock()
	defer w.jobGroupLock.RUnlock()
	job, ok := w.jobGroups[id]
	if !ok {
		return 0, fmt.Errorf("group id %s not found", id)
	}
	return job.completionCount, nil
}

func (w *fileJobPool[T]) JobGroupComplete(id string) error {
	slog.Info("fileJobPool[T] - jobGroup complete", "execution id", id)
	// get the jobGroup
	w.jobGroupLock.RLock()
	c, ok := w.jobGroups[id]
	w.jobGroupLock.RUnlock()
	if !ok {
		slog.Error("JobGroupComplete - job group not found", "execution", id)
		return fmt.Errorf("job group id %s not found", id)
	}
	// close the done channel to signal the scheduler to exit
	close(c.done)
	return nil
}

func (w *fileJobPool[T]) Close() {
	slog.Info("closing fileJobPool[T]", "job pool", w)
	// close the close channel to signal to the job schedulers to exit
	close(w.closing)
	// close the error channel to terminate the error reader
	close(w.errorChan)
	// do not close the job channel - the workers will terminate when `closing` is closed
}

// scheduleJob is schedules a jobGroup to be processed
func (w *fileJobPool[T]) scheduler(g *jobGroup[T]) {
	for {
		// try to write to the jobGroup channel
		// if we can't, wait for a jobGroup to be processed

		// build the filename we assume the filename is <execution_id>_<chunkNumber>.jsonl
		// this will wait until there is a chunkNumber available to process
		// if the jobGroup is complete, it will return -1
		nextChunk := w.waitForNextChunk(g)
		// if no nextChunk returned, either the writer is closing or the jobGroup is complete
		if nextChunk == -1 {
			slog.Debug("exiting scheduler", "execution id", g.id)
			return
		}

		// send the jobGroup to the workers
		// do in a goroutine so we can also check for completion/closure
		j := fileJob[T]{
			groupId:         g.id,
			chunkNumber:     nextChunk,
			completionCount: &g.completionCount,
			payload:         g.payload,
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
			//slog.Debug("sent jobGroup to worker", "chunk", j.chunkNumber, "completion count", *j.completionCount)
			// so we sent a jobGroup
			// update the next chunkNumber
			g.nextChunkIndex++

		// is Writer closing?
		case <-w.closing:
			slog.Debug("write is closing - exiting scheduler", "execution id", g.id)
			return
			// Note we do not check <-jobGroup.done as the jobGroup cannot be done before all chunks are processed
			// and if the jobGroup was complete we would have returned -1 from waitForNextChunk
		}
	}
}

func (w *fileJobPool[T]) waitForNextChunk(job *jobGroup[T]) int {
	// if we have chunks available, build a filename from the next chunkNumber
	if job.nextChunkIndex < len(job.chunks) {
		return job.chunks[job.nextChunkIndex]
	}

	// so there are no chunks available
	// wait for chunkWrittenSignal to be signalled

	// do in a goroutine so we can also check for completion/closure
	var chunkChan = make(chan struct{})
	go func() {
		// wait for chunkWrittenSignal to be signalled
		job.chunkWrittenSignal.L.Lock()
		job.chunkWrittenSignal.Wait()
		job.chunkWrittenSignal.L.Unlock()
		close(chunkChan)
	}()

	select {
	case <-chunkChan:
		// get chunkNumber
		job.chunkLock.RLock()
		defer job.chunkLock.RUnlock()

		// NOTE: trim the buffer
		job.chunks = job.chunks[job.nextChunkIndex:]
		// update the index to point to the start of the trimmed buffer
		job.nextChunkIndex = 0

		if len(job.chunks) == 0 {
			slog.Warn("no more chunks - jobGroup is done", "execution id", job.id)
			// no more chunks - jobGroup is done
			return -1
		}
		// we have new chunks - return the next
		return job.chunks[job.nextChunkIndex]

	case <-w.closing:
		// Writer is closing
		return -1
	case <-job.done:
		// jobGroup is done
		return -1
	}
}

func (w *fileJobPool[T]) readJobErrors() {
	for err := range w.errorChan {
		// find the jobGroup and add the error
		w.jobGroupLock.RLock()
		jobGroup, ok := w.jobGroups[err.jobGroupId]
		w.jobGroupLock.RUnlock()
		if !ok {
			slog.Error("failed to set error for jobGroup %s - job group not found", "execution id", err.jobGroupId)
			continue
		}
		jobGroup.errorsLock.Lock()
		jobGroup.errors = append(jobGroup.errors, err.err)
		jobGroup.errorsLock.Unlock()
	}
}
