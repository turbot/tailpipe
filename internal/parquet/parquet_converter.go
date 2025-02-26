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

// ParquetConverter struct represents all the conversions that need to be done for a single collection
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type ParquetConverter struct {
	// the execution id
	id string

	// the index of the last chunk number
	maxIndex int64
	// the index into 'chunks' of the next chunk number to process
	nextIndex int64

	// the number of chunks processed so far
	completionCount int32
	// the number of rows written
	rowCount int64

	// The channel to send execution to the workers
	jobChan chan *simpleParquetJob

	// WaitGroup to track job completion
	wg sync.WaitGroup
	// sync.Cond to wait for the next chunkNumber to be available
	chunkWrittenSignal *sync.Cond
	// the cancel function for the context used to manage the job
	cancel context.CancelFunc

	// the source file location
	sourceDir string
	// the dest file location
	destDir string
	// helper to provide unique file roots
	fileRootProvider *FileRootProvider

	// the format string for the conversion query will be the same for all chunks - build once and store
	viewQueryFormat string

	// error which occurred during execution
	errors     []error
	errorsLock sync.RWMutex

	// the table schema - populated when the first chunk arrives if the schema is not already complete
	schema    *schema.TableSchema
	schemaMut sync.RWMutex

	// the partition being collected
	Partition *config.Partition
}

func NewParquetConverter(ctx context.Context, cancel context.CancelFunc, executionId string, partition *config.Partition, sourceDir string, schema *schema.TableSchema) (*ParquetConverter, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	w := &ParquetConverter{
		id:                 executionId,
		chunkWrittenSignal: sync.NewCond(&sync.Mutex{}),
		maxIndex:           -1,

		Partition: partition,
		jobChan:   make(chan *simpleParquetJob, parquetWorkerCount*2),
		cancel:    cancel,

		sourceDir: sourceDir,
		destDir:   destDir,
		schema:    schema,

		fileRootProvider: &FileRootProvider{},
	}

	// start the goroutine to schedule the jobs
	go w.scheduler(ctx)

	// start the workers
	for i := 0; i < parquetWorkerCount; i++ {
		wk, err := newParquetConversionWorkerSimple(w)
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
	w.cancel()
}

//
//func (w *ParquetConverter) JobGroupComplete(id string) error {
//	slog.Info("ParquetConverter - ParquetConverter complete", "execution id", id)
//	// close the done channel to signal the scheduler to exit
//	close(w.done)
//	return nil
//}

// AddChunk adds a new chunk to the list of chunks to be processed
// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
// signal the scheduler that chunks are available
func (w *ParquetConverter) AddChunk(executionId string, chunks ...int) error {
	if w.maxIndex == -1 {
		// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
		err := w.inferSchemaIfNeeded(executionId, chunks)
		if err != nil {
			return err
		}

		// now we are sure to have a complete schema, build the view query

		w.viewQueryFormat = buildViewQuery(w.schema)

	}
	// add the chunks to the list
	atomic.AddInt64(&w.maxIndex, int64(len(chunks)))

	// signal the scheduler that there are new chunks
	w.chunkWrittenSignal.L.Lock()
	w.chunkWrittenSignal.Broadcast()
	w.chunkWrittenSignal.L.Unlock()

	return nil
}

func (w *ParquetConverter) GetRowCount() int64 {
	return w.rowCount
}

// WaitForCompletion waits for all jobs to be processed or for the context to be cancelled
func (w *ParquetConverter) WaitForCompletion(ctx context.Context) {
	// wait for the wait group within a goroutine so we can also check the context
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	// check whether the context is cancelled or all jobs are processed
	select {
	case <-ctx.Done():
		fmt.Println("Context cancelled.")
	case <-done:
		fmt.Println("All jobs processed.")
	}
}

// the scheduler is responsible for sending jobs to the workere
// it listens for signals on the chunkWrittenSignal channel and enqueues jobs when they arrive
func (w *ParquetConverter) scheduler(ctx context.Context) {
	// listen to the chunkWrittenSignal in a goroutine and raise an event on the chunkChannel
	// this allows us to also check context cancellation
	chunkChannel := make(chan struct{})
	go func() {
		for {
			w.chunkWrittenSignal.L.Lock()
			w.chunkWrittenSignal.Wait()
			w.chunkWrittenSignal.L.Unlock()

			select {
			case <-ctx.Done(): // Ensure clean exit
				fmt.Println("Stopping scheduler.")
				return
			case chunkChannel <- struct{}{}:
			}
		}
	}()

	// select either a signal from the chunkWrittenSignal or a signal from the context
	for {
		select {
		case <-ctx.Done():
			fmt.Println("scheduler shutting down.")
			// Now it's safe to close the queue
			close(w.jobChan)

			return
		case <-chunkChannel:
			// if there are jobs to enqueue, do so

			// check if we have any chunks to process
			currentNext := atomic.LoadInt64(&w.nextIndex)
			currentMax := atomic.LoadInt64(&w.maxIndex)
			if currentNext <= currentMax {
				// so we have a chunk to process

				// increment the wait group
				w.wg.Add(1)

				// send the job to the worker
				w.jobChan <- &simpleParquetJob{currentNext}

				fmt.Printf("Job enqueued: %d\n", currentNext)

				// increment the nextIndex
				// NOTE: as this function is the only place where nextIndex is incremented,
				// we can safely read and increment it without a lock without risk of a race condition
				atomic.AddInt64(&w.nextIndex, 1)
			}
		}
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

func (w *ParquetConverter) addJobErrors(errors ...error) {
	w.errorsLock.Lock()
	w.errors = append(w.errors, errors...)
	w.errorsLock.Unlock()
}
