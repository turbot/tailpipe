package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/turbot/pipe-fittings/v2/backend"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
)

const chunkBufferLength = 1000

// Converter struct executes all the conversions for a single collection
// it therefore has a unique execution executionId, and will potentially convert of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunk number
type Converter struct {
	// the execution executionId
	executionId string

	// the file scheduledChunks numbers available to process
	scheduledChunks []int32

	scheduleLock sync.Mutex
	processLock  sync.Mutex

	// waitGroup to track job completion
	// this is incremented when a file is scheduled and decremented when the file is processed
	wg sync.WaitGroup

	// the number of jsonl files processed so far
	//fileCount int32

	// the number of conversions executed
	//conversionCount int32

	// the number of rows written
	rowCount int64
	// the number of rows which were NOT converted due to conversion errors encountered
	failedRowCount int64

	// the source file location
	sourceDir string
	// the dest file location
	destDir string

	// the format string for the query to read the JSON scheduledChunks - this is reused for all scheduledChunks,
	// with just the filename being added when the query is executed
	readJsonQueryFormat string

	// the table conversionSchema - populated when the first chunk arrives if the conversionSchema is not already complete
	conversionSchema *schema.ConversionSchema
	// the source schema - which may be partial - used to build the full conversionSchema
	// we store separately for the purpose of change detection
	tableSchema *schema.TableSchema

	// viewQueryOnce ensures the schema inference only happens once for the first chunk,
	// even if multiple scheduledChunks arrive concurrently. Combined with schemaWg, this ensures
	// all subsequent scheduledChunks wait for the initial schema inference to complete before proceeding.
	viewQueryOnce sync.Once
	// schemaWg is used to block processing of subsequent scheduledChunks until the initial
	// schema inference is complete. This ensures all scheduledChunks wait for the schema
	// to be fully initialized before proceeding with their processing.
	schemaWg sync.WaitGroup

	// the partition being collected
	Partition *config.Partition
	// func which we call with updated row count
	statusFunc func(int64, int64, ...error)

	// the DuckDB database connection - this must have a ducklake attachment
	db *DuckDb
}

func NewParquetConverter(ctx context.Context, cancel context.CancelFunc, executionId string, partition *config.Partition, sourceDir string, tableSchema *schema.TableSchema, statusFunc func(int64, int64, ...error), db *DuckDb) (*Converter, error) {
	// get the data dir - this will already have been created by the config loader
	destDir := config.GlobalWorkspaceProfile.GetDataDir()

	// normalise the table schema to use lowercase column names
	tableSchema.NormaliseColumnTypes()

	w := &Converter{
		executionId:     executionId,
		scheduledChunks: make([]int32, 0, chunkBufferLength), // Pre-allocate reasonable capacity
		Partition:       partition,
		sourceDir:       sourceDir,
		destDir:         destDir,
		tableSchema:     tableSchema,
		statusFunc:      statusFunc,
		db:              db,
	}

	// done
	return w, nil
}

// AddChunk adds a new chunk to the list of scheduledChunks to be processed
// if this is the first chunk, determine if we have a full conversionSchema yet and if not infer from the chunk
// signal the scheduler that `scheduledChunks are available
func (w *Converter) AddChunk(executionId string, chunk int32) error {
	var err error

	// wait on the schemaWg to ensure that schema inference is complete before processing the chunk
	w.schemaWg.Wait()

	// Execute schema inference exactly once for the first chunk.
	// The WaitGroup ensures all subsequent scheduledChunks wait for this to complete.
	// If schema inference fails, the error is captured and returned to the caller.
	w.viewQueryOnce.Do(func() {
		err = w.onFirstChunk(executionId, chunk)
	})
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	// lock the schedule lock to ensure that we can safely add to the scheduled scheduledChunks
	w.scheduleLock.Lock()
	// add to scheduled scheduledChunks
	w.scheduledChunks = append(w.scheduledChunks, chunk)
	w.scheduleLock.Unlock()

	// increment the wait group to track the scheduled chunk
	w.wg.Add(1)

	// ok try to lock the process lock - that will fail if another process is running
	if w.processLock.TryLock() {
		// and process = we now have the process lock
		// NOTE: process chunks will keep processing as long as there are scheduledChunks to process, including
		// scheduledChunks that were scheduled while we were processing
		go w.processAllChunks()
	}

	return nil
}

// getChunksToProcess returns the chunks to process, up to a maximum of maxChunksToProcess
// it also trims the scheduledChunks to remove the processed chunks
func (w *Converter) getChunksToProcess() []int32 {
	// now determine if there are more chunks to process
	w.scheduleLock.Lock()
	defer w.scheduleLock.Unlock()

	// provide a mechanism to limit the max chunks we process at once
	// a high value for this seems fine (it's possible we do not actually need a limit at all)
	const maxChunksToProcess = 2000
	var chunksToProcess []int32
	if len(w.scheduledChunks) > maxChunksToProcess {
		slog.Debug("Converter.AddChunk limiting chunks to process to max", "scheduledChunks", len(w.scheduledChunks), "maxChunksToProcess", maxChunksToProcess)
		chunksToProcess = w.scheduledChunks[:maxChunksToProcess]
		// trim the scheduled chunks to remove the processed chunks
		w.scheduledChunks = w.scheduledChunks[maxChunksToProcess:]
	} else {
		slog.Debug("Converter.AddChunk processing all scheduled chunks", "scheduledChunks", len(w.scheduledChunks))
		chunksToProcess = w.scheduledChunks
		// clear the scheduled chunks
		w.scheduledChunks = nil
	}
	return chunksToProcess
}

// onFirstChunk is called when the first chunk is added to the converter
// it is responsible for building the conversion schema if it does not already exist
// (we must wait for the first chunk as we may need to infer the schema from the chunk data)
// once the conversion schema is built, we can create the DuckDB table for this partition and build the
// read query format string that we will use to read the JSON data from the file
func (w *Converter) onFirstChunk(executionId string, chunk int32) error {
	w.schemaWg.Add(1)
	defer w.schemaWg.Done()
	if err := w.buildConversionSchema(executionId, chunk); err != nil {
		// err will be returned by the parent function
		return err
	}
	// create the DuckDB table for this partition if it does not already exist
	if err := EnsureDuckLakeTable(w.conversionSchema.Columns, w.db, w.Partition.TableName); err != nil {
		return fmt.Errorf("failed to create DuckDB table: %w", err)
	}
	w.readJsonQueryFormat = buildReadJsonQueryFormat(w.conversionSchema, w.Partition)

	return nil
}

// WaitForConversions waits for all jobs to be processed or for the context to be cancelled
func (w *Converter) WaitForConversions(ctx context.Context) error {
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
		return ctx.Err()
	case <-done:
		slog.Info("WaitForConversions - all jobs processed.")
		return nil
	}
}

// addJobErrors calls the status func with any job errors, first summing the failed rows in any conversion errors
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

// CheckTableSchema checks if the specified table exists in the DuckDB database and compares its schema with the
// provided schema.
// it returns a TableSchemaStatus indicating whether the table exists, whether the schema matches, and any differences.
// THis is not used at present but will be used when we implement ducklake schema evolution handling
func (w *Converter) CheckTableSchema(db *sql.DB, tableName string, conversionSchema schema.ConversionSchema) (TableSchemaStatus, error) {
	// Check if table exists
	exists, err := w.tableExists(db, tableName)
	if err != nil {
		return TableSchemaStatus{}, err
	}

	if !exists {
		return TableSchemaStatus{}, nil
	}

	// Get existing schema
	existingSchema, err := w.getTableSchema(db, tableName)
	if err != nil {
		return TableSchemaStatus{}, fmt.Errorf("failed to retrieve schema: %w", err)
	}

	// Use constructor to create status from comparison
	diff := NewTableSchemaStatusFromComparison(existingSchema, conversionSchema)
	return diff, nil
}

func (w *Converter) tableExists(db *sql.DB, tableName string) (bool, error) {
	sanitizedTableName, err := backend.SanitizeDuckDBIdentifier(tableName)
	if err != nil {
		return false, fmt.Errorf("invalid table name %s: %w", tableName, err)
	}
	//nolint:gosec // table name is sanitized
	query := fmt.Sprintf("select exists (select 1 from information_schema.tables where table_name = '%s')", sanitizedTableName)
	var exists int
	if err := db.QueryRow(query).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

func (w *Converter) getTableSchema(db *sql.DB, tableName string) (map[string]schema.ColumnSchema, error) {
	query := fmt.Sprintf("pragma table_info(%s);", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaMap := make(map[string]schema.ColumnSchema)
	for rows.Next() {
		var name, dataType string
		var notNull, pk int
		var dfltValue sql.NullString

		if err := rows.Scan(&name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, err
		}

		schemaMap[name] = schema.ColumnSchema{
			ColumnName: name,
			Type:       dataType,
		}
	}

	return schemaMap, nil
}
