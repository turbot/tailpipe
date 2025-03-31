package parquet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
)

const parquetWorkerCount = 5
const chunkBufferLength = 1000

// Converter struct executes all the conversions for a single collection
// it therefore has a unique execution id, and will potentially convert of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunk number
type Converter struct {
	// the execution id
	id string

	// the file chunks numbers available to process
	chunks      []int
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

	// the table schema - populated when the first chunk arrives if the schema is not already complete
	schema        *schema.TableSchema
	schemaMut     sync.RWMutex
	viewQueryOnce sync.Once

	// the partition being collected
	Partition *config.Partition
	// func which we call with updated row count
	statusFunc func(int64, int64, ...error)
}

func NewParquetConverter(ctx context.Context, cancel context.CancelFunc, executionId string, partition *config.Partition, sourceDir string, schema *schema.TableSchema, statusFunc func(int64, int64, ...error)) (*Converter, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	w := &Converter{
		id:               executionId,
		chunks:           make([]int, 0, chunkBufferLength), // Pre-allocate reasonable capacity
		Partition:        partition,
		jobChan:          make(chan *parquetJob, parquetWorkerCount*2),
		cancel:           cancel,
		sourceDir:        sourceDir,
		destDir:          destDir,
		schema:           schema,
		statusFunc:       statusFunc,
		fileRootProvider: &FileRootProvider{},
	}
	// create the condition variable using the same lock
	w.chunkSignal = sync.NewCond(&w.chunkLock)

	// start the goroutine to schedule the jobs
	go w.scheduler(ctx)

	// start the workers
	for range parquetWorkerCount {
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

func (w *Converter) Close() {
	slog.Info("closing Converter")
	// close the close channel to signal to the job schedulers to exit
	w.cancel()
}

// AddChunk adds a new chunk to the list of chunks to be processed
// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
// signal the scheduler that `chunks are available
func (w *Converter) AddChunk(executionId string, chunk int) error {
	w.chunkLock.Lock()
	var viewQueryError error
	// when we receive the first chunk, infer the schema
	w.viewQueryOnce.Do(func() {
		if viewQueryError = w.inferSchemaIfNeeded(executionId, chunk); viewQueryError != nil {
			w.chunkLock.Unlock()
			return
		}
		w.viewQueryFormat = buildViewQuery(w.schema)
	})

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
		case w.jobChan <- &parquetJob{chunkNumber: int64(chunk)}:
		}
	}
}

func (w *Converter) getNextChunk() (int, bool) {
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

func (w *Converter) inferSchemaIfNeeded(executionID string, chunk int) error {
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
			s, err := w.inferChunkSchema(executionID, chunk)
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

func (w *Converter) inferChunkSchema(executionId string, chunkNumber int) (*schema.TableSchema, error) {
	jsonFileName := table.ExecutionIdToFileName(executionId, chunkNumber)
	filePath := filepath.Join(w.sourceDir, jsonFileName)
	return w.inferSchemaForJSONLFile(filePath)
}
func (w *Converter) inferSchemaForJSONLFile(filePath string) (*schema.TableSchema, error) {
	// Open DuckDB connection
	db, err := database.NewDuckDb()
	if err != nil {
		log.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Query to infer schema using json_structure
	query := `
		select json_structure(json)::varchar as schema 
		from read_json_auto(?) 
		limit 1;
	`

	var schemaStr string
	err = db.QueryRow(query, filePath).Scan(&schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Parse the schema JSON
	var fields map[string]string
	if err := json.Unmarshal([]byte(schemaStr), &fields); err != nil {
		return nil, fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	// Convert to TableSchema
	res := &schema.TableSchema{
		AutoMapSourceFields: false,
		Columns:             make([]*schema.ColumnSchema, 0, len(fields)),
	}

	// Convert each field to a column schema
	for name, typ := range fields {
		res.Columns = append(res.Columns, &schema.ColumnSchema{
			SourceName: name,
			ColumnName: name,
			Type:       typ,
		})
	}

	return res, nil
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
