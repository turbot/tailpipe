package parquet

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
	"log"
	"log/slog"
	"path/filepath"
	"sync"
)

const parquetWorkerCount = 5

type parquetJobError struct {
	executionId string
	err         error
}

// ParquetJobPool struct represents all the conversions that need to be done for a single collection
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type ParquetJobPool struct {
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
	// channel to mark ParquetJobPool completion
	done chan struct{}

	schema    *schema.RowSchema
	schemaMut sync.RWMutex

	Partition *config.Partition
}

func NewParquetJobPool(executionId string, partition *config.Partition, sourceDir string, schema *schema.RowSchema) (*ParquetJobPool, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	w := &ParquetJobPool{
		id:                 executionId,
		chunkWrittenSignal: sync.NewCond(&sync.Mutex{}),
		done:               make(chan struct{}),
		Partition:          partition,
		jobChan:            make(chan parquetJob, parquetWorkerCount*2),
		errorChan:          make(chan parquetJobError),
		closing:            make(chan struct{}),
		workerCount:        parquetWorkerCount,
		sourceDir:          sourceDir,
		destDir:            destDir,
		schema:             schema,

		fileRootProvider: &FileRootProvider{},
	}

	// start the goroutine to schedule the jobs
	go w.scheduler()
	// start a goroutine to read the error channel
	go w.readJobErrors()
	// start the workers
	for i := 0; i < w.workerCount; i++ {
		wk, err := newParquetConversionWorker(w.jobChan, w.errorChan, w.sourceDir, w.destDir, w.fileRootProvider)
		if err != nil {
			return nil, fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start()
	}

	// done
	return w, nil
}

func (w *ParquetJobPool) Close() {
	slog.Info("closing ParquetJobPool")
	// close the close channel to signal to the job schedulers to exit
	close(w.closing)
	// close the error channel to terminate the error reader
	//w.errorChan <- nil
	// TODO FIXME worker may still be sending

	// do not close the job channel - the workers will terminate when `closing` is closed
}

func (w *ParquetJobPool) JobGroupComplete(id string) error {
	slog.Info("ParquetJobPool - ParquetJobPool complete", "execution id", id)
	// close the done channel to signal the scheduler to exit
	close(w.done)
	return nil
}

func (w *ParquetJobPool) AddChunk(executionId string, chunks ...int) error {
	// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
	err := w.inferSchemaIfNeeded(executionId, chunks)
	if err != nil {
		return err
	}

	// add the chunks to the list
	w.chunkLock.Lock()
	w.chunks = append(w.chunks, chunks...)
	w.chunkLock.Unlock()

	// signal the scheduler that there are new chunks
	w.chunkWrittenSignal.L.Lock()
	w.chunkWrittenSignal.Broadcast()
	w.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *ParquetJobPool) GetChunksWritten(id string) (int32, error) {
	// if the job has errors, terminate
	w.errorsLock.RLock()
	defer w.errorsLock.RUnlock()
	if len(w.errors) > 0 {
		err := errors.Join(w.errors...)
		return -1, fmt.Errorf("job group %s has errors: %w", id, err)
	}
	return w.completionCount, nil
}

func (w *ParquetJobPool) GetRowCount() (int64, error) {
	return w.rowCount, nil
}

func (w *ParquetJobPool) inferSchemaIfNeeded(executionID string, chunks []int) error {
	//  determine if we have a full schema yet and if not infer from the chunk
	// NOTE: schema mode will be MUTATED once we infer it

	// TODO #testing test this https://github.com/turbot/tailpipe/issues/108

	// first get read lock
	w.schemaMut.RLock()
	// is the schema complete (i.e. we are NOT automapping source columns and we have all types defined)
	complete := w.schema.Complete()
	w.schemaMut.RUnlock()

	// do we have the full schema?
	if !complete {
		// get write lock
		w.schemaMut.Lock()
		// check again if schema is still not full (to avoid race condition as another worker may have filled it)
		if !w.schema.Complete() {
			// do the inference
			s, err := w.inferChunkSchema(executionID, chunks[0])
			if err != nil {
				return fmt.Errorf("failed to infer schema from first JSON file: %w", err)
			}
			w.schema.InitialiseFromInferredSchema(s)
		}
		w.schemaMut.Unlock()
	}
	return nil
}

func (w *ParquetJobPool) inferChunkSchema(executionId string, chunkNumber int) (*schema.RowSchema, error) {
	jsonFileName := table.ExecutionIdToFileName(executionId, chunkNumber)
	filePath := filepath.Join(w.sourceDir, jsonFileName)

	// Open DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Use DuckDB to describe the schema of the JSONL file
	query := `SELECT column_name, column_type FROM (DESCRIBE (SELECT * FROM read_json_auto(?)))`

	rows, err := db.Query(query, filePath)

	//rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query JSON schema: %w", err)
	}
	defer rows.Close()

	var res = &schema.RowSchema{
		// NOTE: set autoMap to false as we have inferred the schema
		AutoMapSourceFields: false,
	}

	// Read the results
	for rows.Next() {
		var name, dataType string
		err := rows.Scan(&name, &dataType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		// Append inferred columns to the schema
		res.Columns = append(res.Columns, &schema.ColumnSchema{
			SourceName: name,
			ColumnName: name,
			Type:       dataType,
		})
	}

	// Check for any errors from iterating over rows
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed during rows iteration: %w", err)
	}

	return res, nil

}

// scheduleJob is schedules a ParquetJobPool to be processed
func (w *ParquetJobPool) scheduler() {
	for {
		// try to write to the ParquetJobPool channel
		// if we can't, wait for a ParquetJobPool to be processed

		// build the filename we assume the filename is <execution_id>_<chunkNumber>.jsonl
		// this will wait until there is a chunkNumber available to process
		// if the ParquetJobPool is complete, it will return -1
		nextChunk := w.waitForNextChunk()
		// if no nextChunk returned, either the writer is closing or the ParquetJobPool is complete
		if nextChunk == -1 {
			slog.Debug("exiting scheduler", "execution id", w.id)
			return
		}

		// send the ParquetJobPool to the workers
		// do in a goroutine so we can also check for completion/closure
		j := parquetJob{
			groupId:         w.id,
			chunkNumber:     nextChunk,
			completionCount: &w.completionCount,
			rowCount:        &w.rowCount,
			Partition:       w.Partition,
			SchemaFunc:      w.GetSchema,
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
			//slog.Debug("sent ParquetJobPool to worker", "chunk", j.chunkNumber, "completion count", *j.completionCount)
			// so we sent a ParquetJobPool
			// update the next chunkNumber
			w.nextChunkIndex++

		// is Writer closing?
		case <-w.closing:
			slog.Debug("write is closing - exiting scheduler", "execution id", w.id)
			return
			// Note we do not check <-ParquetJobPool.done as the ParquetJobPool cannot be done before all chunks are processed
			// and if the ParquetJobPool was complete we would have returned -1 from waitForNextChunk
		}
	}
}

func (w *ParquetJobPool) waitForNextChunk() int {
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
			slog.Warn("no more chunks - ParquetJobPool is done", "execution id", w.id)
			// no more chunks - ParquetJobPool is done
			return -1
		}
		// we have new chunks - return the next
		return w.chunks[w.nextChunkIndex]

	case <-w.closing:
		// Writer is closing
		return -1
	case <-w.done:
		// ParquetJobPool is done
		return -1
	}
}

func (w *ParquetJobPool) readJobErrors() {
	for err := range w.errorChan {
		w.errorsLock.Lock()
		w.errors = append(w.errors, err.err)
		w.errorsLock.Unlock()
	}
}

func (w *ParquetJobPool) GetSchema() *schema.RowSchema {
	return w.schema
}
