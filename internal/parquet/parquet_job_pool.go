package parquet

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
	"log"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const parquetWorkerCount = 5

// ParquetConverter struct represents all the conversions that need to be done for a single collection
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type ParquetConverter struct {
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

	// the source file location
	sourceDir string
	// the dest file location
	destDir string
	// helper to provide unique file roots
	fileRootProvider *FileRootProvider

	// sync.Cond to wait for the next chunkNumber to be available
	chunkWrittenSignal *sync.Cond

	// error which occurred during execution
	errors     []error
	errorsLock sync.RWMutex

	// the table schema - populated when the first chunk arrives if the schema is not already complete
	schema    *schema.TableSchema
	schemaMut sync.RWMutex

	// the partition being collected
	Partition *config.Partition
}

func NewParquetJobPool(ctx context.Context, cancel context.CancelFunc, executionId string, partition *config.Partition, sourceDir string, schema *schema.TableSchema) (*ParquetConverter, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	w := &ParquetConverter{
		id:                 executionId,
		chunkWrittenSignal: sync.NewCond(&sync.Mutex{}),

		Partition: partition,
		jobChan:   make(chan parquetJob, parquetWorkerCount*2),

		sourceDir: sourceDir,
		destDir:   destDir,
		schema:    schema,

		fileRootProvider: &FileRootProvider{},
	}

	// start the goroutine to schedule the jobs
	go w.scheduler(ctx)

	// start the workers
	for i := 0; i < parquetWorkerCount; i++ {
		wk, err := newParquetConversionWorker(w)
		if err != nil {
			return nil, fmt.Errorf("failed to create worker: %w", err)

		}
		// start the worker
		go wk.start(ctx)
	}

	// done
	return w, nil
}

func (w *ParquetConverter) Close() {
	slog.Info("closing ParquetConverter")
	// close the close channel to signal to the job schedulers to exit

}

// AddChunk adds a new chunk to the list of chunks to be processed
// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
// signal the scheduler that chunks are available
func (w *ParquetConverter) AddChunk(executionId string, chunks ...int) error {
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

// WaitForCompletion waits for all jobs to be processed or for the context to be cancelled
func (w *ParquetConverter) WaitForCompletion(ctx context.Context) {
	// check whether the context is cancelled or all jobs are processed
	select {
	case <-ctx.Done():
		slog.Info("WaitForCompletion - context cancelled.")
	}
}

func (w *ParquetConverter) inferSchemaIfNeeded(executionID string, chunks []int) error {
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
	// now validate the schema is complete - we should have types for all columns
	// (if we do not that indicates a custom table definition was used which does not specify types for all optional fields -
	// this should have caused a config validation error earlier on
	return w.schema.EnsureComplete()
}

func (w *ParquetConverter) inferChunkSchema(executionId string, chunkNumber int) (*schema.TableSchema, error) {
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

	var res = &schema.TableSchema{
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

// the scheduler is responsible for sending jobs to the workere
// it listens for signals on the chunkWrittenSignal channel and enqueues jobs when they arrive
func (w *ParquetConverter) scheduler(ctx context.Context) {
	for {
		// try to write to the ParquetConverter channel
		// if we can't, wait for a ParquetConverter to be processed

		// build the filename we assume the filename is <execution_id>_<chunkNumber>.jsonl
		// this will wait until there is a chunkNumber available to process
		// if the ParquetConverter is complete, it will return -1
		nextChunk := w.waitForNextChunk()
		// if no nextChunk returned, either the writer is closing or the ParquetConverter is complete
		if nextChunk == -1 {
			slog.Debug("exiting scheduler", "execution id", w.id)
			return
		}

		// send the ParquetConverter to the workers
		// do in a goroutine so we can also check for completion/closure
		j := parquetJob{
			groupId:         w.id,
			chunkNumber:     nextChunk,
			completionCount: &w.completionCount,
			rowCount:        &w.rowCount,
			Partition:       w.Partition,
		}
		// TODO #conversion is this costly to do thousands of times?
		sendChan := make(chan struct{})
		go func() {
			select {
			case w.jobChan <- j:
				close(sendChan)
			}
		}()

		select {
		// wait for send completion
		case <-sendChan:
			//slog.Debug("sent ParquetConverter to worker", "chunk", j.chunkNumber, "completion count", *j.completionCount)
			// so we sent a ParquetConverter
			// update the next chunkNumber
			w.nextChunkIndex++

		}
	}
}

func (w *ParquetConverter) waitForNextChunk() int {
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
			slog.Warn("no more chunks - ParquetConverter is done", "execution id", w.id)
			// no more chunks - ParquetConverter is done
			return -1
		}
		// we have new chunks - return the next
		return w.chunks[w.nextChunkIndex]

	}
}

func (w *ParquetConverter) addJobErrors(errors ...error) {
	w.errorsLock.Lock()
	w.errors = append(w.errors, errors...)
	w.errorsLock.Unlock()
}

// updateRowCount atomically increments the row count and calls the statusFunc
func (w *ParquetConverter) updateRowCount(count int64) {
	atomic.AddInt64(&w.rowCount, count)
}

func (w *ParquetConverter) GetRowCount() (int64, error) {
	return atomic.LoadInt64(&w.rowCount), nil

}
