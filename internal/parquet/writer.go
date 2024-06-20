package parquet

import (
	"fmt"
	"log/slog"
	"sync"
)

/*
Writer is a parquet writer that converts json files to parquet files, following a specific hiv structure:

{tp_collection_type}#div#tp_collection={tp_collection}#div#tp_connection={tp_connection}#div#tp_year={tp_year}#div#tp_month={tp_month}#div#tp_day={tp_day}#div#{execution_id}.parquet

Tailpipe will collect and then compact logs - these are deliberately different phases.
Collection creates a series of smaller parquet files added to the specific day directory.
Compaction will then combine those files (per-day) into a single larger file.
File changes will be done as temp files with instant (almost transactional) renaming operations
- allowing DuckDB to use the files with minimal chance of locking / parse errors.
*/
type Writer struct {
	// The queue of collections to be processed
	// this is a map keyed by execution id and the value is the list of json files to be processed
	collections map[string]*collection
	// The lock to protect the collections map
	collectionsLock sync.RWMutex

	// The channel to send collections to the workers
	jobChan chan job
	// The channel to receive errors from the workers
	errorChan chan error

	// the source file location
	sourceDir string
	// the dest file location
	destDir string

	// channel to indicate we are closing
	closing chan struct{}

	// the number of workers
	workerCount int
}

func NewWriter(sourceDir, destDir string, workers int) (*Writer, error) {
	w := &Writer{
		sourceDir:   sourceDir,
		destDir:     destDir,
		collections: make(map[string]*collection),
		// create worker collection channel
		// amount of buffering is not crucial as the Writer will also be buffering the collections
		// just set to twice number of workers
		jobChan:     make(chan job, workers*2),
		errorChan:   make(chan error),
		closing:     make(chan struct{}),
		workerCount: workers,
	}

	// TODO READ ERROR CHANNEL
	if err := w.Start(); err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to start parquet writer: %w", err)
	}
	return w, nil
}

// Start the parquet Writer - spawn workers
func (w *Writer) Start() error {
	slog.Info("starting parquet Writer", "worker count", w.workerCount)
	// start the workers
	for i := 0; i < w.workerCount; i++ {
		wk, err := newWorker(w.jobChan, w.errorChan, w.sourceDir, w.destDir)
		if err != nil {
			return fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start()
	}

	return nil
}

// StartCollection schedules a collection to be processed
// it adds an entry to the collections map and starts a goroutine to schedule the collection
func (w *Writer) StartCollection(executionId string, t string) error {
	slog.Info("starting collection", "execution id", executionId)
	// we expect this execution id WIL NOT be in the map already
	// if it is, we should return an error
	if _, ok := w.collections[executionId]; ok {
		return fmt.Errorf("execution id %s already exists", executionId)
	}

	job := newCollection(executionId)
	w.collectionsLock.Lock()
	// add the collection to the map
	w.collections[executionId] = job
	w.collectionsLock.Unlock()

	// start a thread to schedule
	// this will terminate when the collection is complete as the collection.done channel will be closed
	go w.scheduler(job)

	return nil
}

// AddChunk adds available chunks to a collection
// this is making the assumpition that all files for a collection are have a filename of format <execution_id>_<chunkNumber>.jsonl
// therefore we only need to pass the chunkNumber number
func (w *Writer) AddChunk(executionID string, chunks ...int) error {
	// get the collection
	w.collectionsLock.RLock()
	collection, ok := w.collections[executionID]
	w.collectionsLock.RUnlock()

	if !ok {
		return fmt.Errorf("execution id %s does not exist", executionID)
	}

	// add the chunks to the collection
	collection.chunkLock.Lock()
	collection.chunks = append(collection.chunks, chunks...)
	collection.chunkLock.Unlock()

	// signal the scheduler that there are new chunks
	collection.chunkWrittenSignal.L.Lock()
	collection.chunkWrittenSignal.Broadcast()
	collection.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *Writer) GetChunksWritten(id string) (int32, error) {
	w.collectionsLock.RLock()
	defer w.collectionsLock.RUnlock()
	job, ok := w.collections[id]
	if !ok {
		return 0, fmt.Errorf("execution id %s not found", id)
	}
	return job.chunksConverted, nil

}

func (w *Writer) CollectionComplete(executionId string) error {
	slog.Info("Writer - collection complete", "execution id", executionId)
	// get the collection
	w.collectionsLock.RLock()
	c, ok := w.collections[executionId]
	w.collectionsLock.RUnlock()
	if !ok {
		slog.Error("CollectionComplete - execution not found", "execution", executionId)
		return fmt.Errorf("execution id %s not found", executionId)
	}
	// close the done channel to signal the scheduler to exit
	close(c.done)
	return nil
}

func (w *Writer) Close() {
	// close the close channel to signal to the job schedulers to exit
	close(w.closing)
	// close the job channel to terminate the workers
	close(w.jobChan)
}

// scheduleJob is schedules a collection to be processed
func (w *Writer) scheduler(c *collection) {
	for {
		// try to write to the collection channel
		// if we can't, wait for a collection to be processed

		// build the filename we assume the filename is <execution_id>_<chunkNumber>.jsonl
		// this will wait until there is a chunkNumber available to process
		// if the collection is complete, it will return -1
		nextChunk := w.waitForNextChunk(c)
		// if no nextChunk returned, either the writer is closing or the collection is complete
		if nextChunk == -1 {
			slog.Debug("exiting scheduler", "execution id", c.executionId)
			return
		}

		// send the collection to the workers
		// do in a goroutine so we can also check for completion/closure
		j := job{
			executionID:     c.executionId,
			chunkNumber:     nextChunk,
			completionCount: &c.chunksConverted,
			// TODO pass through
			collectionType: "benchmark",
		}
		sendChan := make(chan struct{})
		go func() {
			w.jobChan <- j
			close(sendChan)
		}()

		select {
		// wait for send completion
		case <-sendChan:
			slog.Debug("sent collection to worker", "job", j)
			// so we sent a collection
			// update the next chunkNumber
			c.nextChunkIndex++

		// is Writer closing?
		case <-w.closing:
			slog.Debug("write is closing - exiting scheduler", "execution id", c.executionId)
			return
			// Note we do not check <-collection.done as the collection cannot be done before all chunks are processed
			// and if the collection was complete we would have returned -1 from waitForNextChunk
		}
	}
}

func (w *Writer) waitForNextChunk(job *collection) int {
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
		// collection is done
		return -1
	}
}

//func (w *Writer) CombineFiles(executionID string) error {
//	db, err := newDuckDb()
//	if err != nil {
//		return fmt.Errorf("failed to create DuckDB writer: %w", err)
//	}
//
//	// list all the parquet files for the given execution id
//	parquetFiles, err := filepath.Glob(filepath.Join(w.destDir, fmt.Sprintf("%s*.parquet", executionID)))
//	if err != nil {
//		return fmt.Errorf("failed to list parquet files: %w", err)
//	}
//	if len(parquetFiles) == 0 {
//		return fmt.Errorf("no parquet files found for execution id %s", executionID)
//	}
//
//	// Create the combined table using the schema of the first Parquet file
//	createTableQuery := fmt.Sprintf("CREATE TABLE combined_table AS SELECT * FROM parquet_scan('%s') LIMIT 0;", parquetFiles[0])
//	_, err = db.Exec(createTableQuery)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Insert data from each Parquet file into the combined table
//	for _, file := range parquetFiles {
//		insertDataQuery := fmt.Sprintf("INSERT INTO combined_table SELECT * FROM parquet_scan('%s');", file)
//		_, err = db.Exec(insertDataQuery)
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//
//	// Export the combined data to a new Parquet file
//	combinedFile := filepath.Join(w.destDir, fmt.Sprintf("%s.parquet", executionID))
//	exportQuery := fmt.Sprintf("COPY combined_table TO '%s' (FORMAT PARQUET);", combinedFile)
//	_, err = db.Exec(exportQuery)
//	if err != nil {
//		log.Fatal(err)
//	}
//	var deleteErrors []error // delete all the source files
//	for _, file := range parquetFiles {
//		if err := os.Remove(file); err != nil {
//			deleteErrors = append(deleteErrors, fmt.Errorf("failed to delete parquet file %s: %w", file, err))
//		}
//	}
//	if len(deleteErrors) > 0 {
//		// TODO #errors what to do with this error?
//		w.errorChan <- fmt.Errorf("failed to individual delete parquet files: %w", errors.Join(deleteErrors...))
//	}
//
//	return nil
//}
