package parquet

import (
	"fmt"
	"log/slog"
	"sync"
)

// fileJobPool is a pool of workers that process file jobs
type fileJobPool struct {
	// The queue of jobGroups to be processed
	// this is a map keyed by execution id and the value is the list of json files to be processed
	jobGroups map[string]*jobGroup
	// The lock to protect the jobGroups map
	jobGroupLock sync.RWMutex

	// The channel to send jobGroups to the workers
	jobChan chan fileJob
	// The channel to receive errors from the workers
	errorChan chan error

	// channel to indicate we are closing
	closing chan struct{}

	// the source file location
	sourceDir string
	// the dest file location
	destDir string

	// the number of workers
	workerCount int
	// function to create a new worker
	newWorker newWorkerFunc
}

// ctor
func newFileJobPool(workers int, sourceDir, destDir string, newWorker newWorkerFunc) *fileJobPool {

	return &fileJobPool{
		jobGroups: make(map[string]*jobGroup),
		// create worker jobGroup channel
		// amount of buffering is not crucial as the Writer will also be buffering the jobGroups
		// just set to twice number of workers
		jobChan:     make(chan fileJob, workers*2),
		errorChan:   make(chan error),
		closing:     make(chan struct{}),
		workerCount: workers,
		sourceDir:   sourceDir,
		destDir:     destDir,
		newWorker:   newWorker,
	}
}

func (w *fileJobPool) Start() error {
	slog.Info("starting parquet Writer", "worker count", w.workerCount)
	// start the workers
	for i := 0; i < w.workerCount; i++ {
		wk, err := w.newWorker(w.jobChan, w.errorChan, w.sourceDir, w.destDir)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start()
	}

	return nil
}

func (w *fileJobPool) StartJobGroup(id, collectionType string) error {
	slog.Info("starting jobGroup", "execution id", id)
	// we expect this execution id WIL NOT be in the map already
	// if it is, we should return an error
	if _, ok := w.jobGroups[id]; ok {
		fmt.Errorf("job group id %s already exists", id)
	}

	jobGroup := newJobGroup(id, collectionType)
	w.jobGroupLock.Lock()
	// add the jobGroup to the map
	w.jobGroups[id] = jobGroup
	w.jobGroupLock.Unlock()

	// start a thread to schedule
	// this will terminate when the jobGroup is complete as the jobGroup.done channel will be closed
	go w.scheduler(jobGroup)
	return nil
}

func (w *fileJobPool) AddChunk(id string, chunks ...int) error {
	// get the jobGroup
	w.jobGroupLock.RLock()
	collection, ok := w.jobGroups[id]
	w.jobGroupLock.RUnlock()

	if !ok {
		return fmt.Errorf("group id %s does not exist", id)
	}

	// add the chunks to the jobGroup
	collection.chunkLock.Lock()
	collection.chunks = append(collection.chunks, chunks...)
	collection.chunkLock.Unlock()

	// signal the scheduler that there are new chunks
	collection.chunkWrittenSignal.L.Lock()
	collection.chunkWrittenSignal.Broadcast()
	collection.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *fileJobPool) GetChunksWritten(id string) (int32, error) {
	w.jobGroupLock.RLock()
	defer w.jobGroupLock.RUnlock()
	job, ok := w.jobGroups[id]
	if !ok {
		return 0, fmt.Errorf("group id %s not found", id)
	}
	return job.completionCount, nil
}

func (w *fileJobPool) JobGroupComplete(id string) error {
	slog.Info("fileJobPool - jobGroup complete", "execution id", id)
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

func (w *fileJobPool) Close() {
	// close the close channel to signal to the job schedulers to exit
	close(w.closing)
	// close the job channel to terminate the workers
	close(w.jobChan)
}

// scheduleJob is schedules a jobGroup to be processed
func (w *fileJobPool) scheduler(g *jobGroup) {
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
		j := fileJob{
			groupId:         g.id,
			chunkNumber:     nextChunk,
			completionCount: &g.completionCount,
			collectionType:  g.collectionType,
		}
		sendChan := make(chan struct{})
		go func() {
			w.jobChan <- j
			close(sendChan)
		}()

		select {
		// wait for send completion
		case <-sendChan:
			slog.Debug("sent jobGroup to worker", "job", j)
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

func (w *fileJobPool) waitForNextChunk(job *jobGroup) int {
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
		// update the index to point to the start ogf the trimmed buffer
		job.nextChunkIndex = 0

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
