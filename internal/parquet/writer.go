package parquet

import (
	"fmt"
	"log/slog"
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

	// the job pool
	jobPool *fileJobPool
}

func NewWriter(sourceDir, destDir string, workers int) (*Writer, error) {
	w := &Writer{
		jobPool: newFileJobPool(workers, sourceDir, destDir, newParquetConversionWorker),
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
	return w.jobPool.Start()

}

// StartCollection schedules a jobGroup to be processed
// it adds an entry to the jobGroups map and starts a goroutine to schedule the jobGroup
func (w *Writer) StartCollection(executionId, collectionType string) error {
	slog.Info("parquet.Writer.StartCollection", "executionId", executionId, "collectionType", collectionType)
	return w.jobPool.StartJobGroup(executionId, collectionType)
}

// AddChunk adds available chunks to a jobGroup
// this is making the assumption that all files for a jobGroup are have a filename of format <execution_id>_<chunkNumber>.jsonl
// therefore we only need to pass the chunkNumber number
func (w *Writer) AddChunk(executionID string, chunks ...int) error {
	return w.jobPool.AddChunk(executionID, chunks...)

}

func (w *Writer) GetChunksWritten(id string) (int32, error) {
	return w.jobPool.GetChunksWritten(id)
}

func (w *Writer) CollectionComplete(executionId string) error {
	return w.jobPool.JobGroupComplete(executionId)
}

func (w *Writer) Close() {
	w.jobPool.Close()
}
