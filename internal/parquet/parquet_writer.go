package parquet

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
)

/*
Writer is a parquet writer that converts json files to parquet files, following a specific hive structure:

{tp_partition_type}#div#tp_partition={tp_partition}#div#tp_index={tp_index}#div#tp_year={tp_year}#div#tp_month={tp_month}#div#tp_day={tp_day}#div#{execution_id}.parquet

Tailpipe will collect and then compact logs - these are deliberately different phases.
Collection creates a series of smaller parquet files added to the specific day directory.
Compaction will then combine those files (per-day) into a single larger file.
File changes will be done as temp files with instant (almost transactional) renaming operations
- allowing DuckDB to use the files with minimal chance of locking / parse errors.
*/
type ParquetWriter struct {
	// the job pool
	jobPool *parquetJobPool
}

func NewWriter(executionId string, partition *config.Partition, schemeFunc func() *schema.RowSchema, updateActiveDuration func(increment time.Duration), sourceDir, destDir string, workers int) (*ParquetWriter, error) {
	w := &ParquetWriter{
		sourceDir:   sourceDir,
		destDir:     destDir,
		workerCount: workers,
		jobPool:     newParquetJobPool(executionId, partition, schemeFunc, updateActiveDuration, workers, sourceDir, destDir),
	}

	err := w.jobPool.StartExecution(executionId, partition, schemeFunc, updateActiveDuration)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// AddJob adds available jobs to a parquetJobPool
// this is making the assumption that all files for a parquetJobPool are have a filename of format <execution_id>_<chunkNumber>.jsonl
// therefore we only need to pass the chunkNumber number
func (w *ParquetWriter) AddJob(executionID string, chunks ...int) error {
	// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
	err := w.inferSchemaIfNeeded(executionID, chunks)
	if err != nil {
		return err
	}

	return w.jobPool.AddChunk(executionID, chunks...)
}

func (w *ParquetWriter) inferSchemaIfNeeded(executionID string, chunks []int) error {
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

func (w *ParquetWriter) GetChunksWritten(id string) (int32, error) {
	return w.jobPool.GetChunksWritten(id)
}

func (w *ParquetWriter) GetRowCount() (int64, error) {
	return w.jobPool.GetRowCount()
}

func (w *ParquetWriter) JobGroupComplete(executionId string) error {
	return w.jobPool.JobGroupComplete(executionId)
}

func (w *ParquetWriter) Close() {
	w.jobPool.Close()
}

func (w *ParquetWriter) GetSchema() *schema.RowSchema {
	return w.schema
}

// SetSchema - NOTE: this may be dynamic (i.e. empty) or partial, in which case, we
// will need to infer the schema from the first JSONL file
func (w *ParquetWriter) SetSchema(rowSchema *schema.RowSchema) {
	w.schema = rowSchema
}

func (w *ParquetWriter) inferChunkSchema(executionId string, chunkNumber int) (*schema.RowSchema, error) {
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
