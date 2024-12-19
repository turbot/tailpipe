package parquet

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
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
type Writer struct {
	// the job pool
	jobPool   *fileJobPool[JobPayload]
	schema    *schema.RowSchema
	schemaMut sync.RWMutex
	sourceDir string
}

func NewWriter(sourceDir, destDir string, workers int) (*Writer, error) {
	w := &Writer{
		sourceDir: sourceDir,
		jobPool:   newFileJobPool(workers, sourceDir, destDir, newParquetConversionWorker),
	}

	if err := w.Start(); err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to start parquet writer: %w", err)
	}
	return w, nil
}

// Start the parquet Writer - spawn workers
func (w *Writer) Start() error {
	return w.jobPool.Start()

}

// StartJobGroup schedules a jobGroup to be processed
// it adds an entry to the jobGroups map and starts a goroutine to schedule the jobGroup
func (w *Writer) StartJobGroup(executionId string, payload JobPayload) error {
	slog.Info("parquet.Writer.StartJobGroup", "jobGroupId", executionId, "partitionName", payload.Partition.UnqualifiedName)

	return w.jobPool.StartJobGroup(executionId, payload)
}

// AddJob adds available jobs to a jobGroup
// this is making the assumption that all files for a jobGroup are have a filename of format <execution_id>_<chunkNumber>.jsonl
// therefore we only need to pass the chunkNumber number
func (w *Writer) AddJob(executionID string, chunks ...int) error {
	// if this is the first chunk, determine if we have a full schema yet and if not infer from the chunk
	err := w.inferSchemaIfNeeded(executionID, chunks)
	if err != nil {
		return err
	}

	return w.jobPool.AddChunk(executionID, chunks...)
}

func (w *Writer) inferSchemaIfNeeded(executionID string, chunks []int) error {
	//  determine if we have a full schema yet and if not infer from the chunk
	// NOTE: schema mode will be MUTATED once we infer it

	// first get read lock
	w.schemaMut.RLock()
	m := w.schema.Mode
	w.schemaMut.RUnlock()

	// do we have the full schema?
	if m != schema.ModeFull {
		// get write lock
		w.schemaMut.Lock()
		// check again if schema is still not full (to avoid race condition)
		if w.schema.Mode != schema.ModeFull {
			// do the inference
			s, err := w.inferSchema(executionID, chunks[0])
			if err != nil {
				return fmt.Errorf("failed to infer schema from first JSON file: %w", err)
			}
			w.SetSchema(s)
		}
		w.schemaMut.Unlock()
	}
	return nil
}

func (w *Writer) GetChunksWritten(id string) (int32, error) {
	return w.jobPool.GetChunksWritten(id)
}

func (w *Writer) GetRowCount(id string) (int64, error) {
	return w.jobPool.GetRowCount(id)
}

func (w *Writer) JobGroupComplete(executionId string) error {
	return w.jobPool.JobGroupComplete(executionId)
}

func (w *Writer) Close() {
	w.jobPool.Close()
}

func (w *Writer) GetSchema() *schema.RowSchema {
	return w.schema
}

// SetSchema - NOTE: this may be dynamic (i.e. empty) or partial, in which case, we
// will need to infer the schema from the first JSONL file
func (w *Writer) SetSchema(rowSchema *schema.RowSchema) {
	w.schema = rowSchema
}

func (w *Writer) inferSchema(executionId string, chunkNumber int) (*schema.RowSchema, error) {
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
		// NOTE: set the mode to full to indicate that we have inferred the schema
		Mode: schema.ModeFull,
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

	// now if a partial schema was provided by the plugin override the inferred schema
	if w.schema.Mode == schema.ModePartial {
		// build a map of the partial schema columns
		var partialSchemaMap = make(map[string]*schema.ColumnSchema)
		for _, c := range w.schema.Columns {
			partialSchemaMap[c.ColumnName] = c
		}
		for _, c := range res.Columns {
			if _, ok := partialSchemaMap[c.ColumnName]; ok {
				slog.Info("Overriding inferred schema with partial schema", "columnName", c.ColumnName, "type", partialSchemaMap[c.ColumnName].Type)
				c.Type = partialSchemaMap[c.ColumnName].Type
			}
		}
	}

	return res, nil

}
