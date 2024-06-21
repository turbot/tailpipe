package parquet

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	"log/slog"
	"os"
	"path/filepath"
)

// parquetConversionWorker is an implementation of worker that converts JSONL files to Parquet
type parquetConversionWorker struct {
	fileWorkerBase
	db *duckDb
}

// ctor
func newParquetConversionWorker(jobChan chan fileJob, errorChan chan error, sourceDir, destDir string) (worker, error) {
	w := &parquetConversionWorker{
		fileWorkerBase: newWorker(jobChan, errorChan, sourceDir, destDir),
	}

	// create a new DuckDB instance
	db, err := newDuckDb()
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB wrapper: %w", err)
	}
	w.db = db
	// set base doWork function
	w.doWorkFunc = w.doJSONToParquetConversion
	w.closeFunc = w.close
	return w, nil

}

func (w *parquetConversionWorker) close() {
	w.db.Close()
}

func (w *parquetConversionWorker) doJSONToParquetConversion(job fileJob) {
	// build the source filename
	jsonFileName := plugin.ExecutionIdToFileName(job.groupId, job.chunkNumber)
	jsonFilePath := filepath.Join(w.sourceDir, jsonFileName)

	slog.Debug("source file", "path", jsonFilePath)

	// process the jobGroup
	err := w.convertFile(jsonFilePath, job.collectionType)
	if err != nil {
		slog.Error("failed to convert file", "error", err)
		w.errorChan <- err
		// TODO abort??
	}

	slog.Debug("converted file successfully, deleting JSONL file", "path", jsonFilePath)

	// delete JSON file (configurable?)
	if err := os.Remove(jsonFilePath); err != nil {
		w.errorChan <- fmt.Errorf("failed to delete JSONL file %s: %w", jsonFilePath, err)
	}
}

// convert the given jsonl file to parquet
func (w *parquetConversionWorker) convertFile(jsonlFilePath, collectionType string) (err error) {
	slog.Debug("worker.convertFile", "jsonlFilePath", jsonlFilePath, "collectionType", collectionType)
	defer slog.Debug("worker.convertFile - done", "error", err)

	// verify the jsonl file has a .jsonl extension
	if filepath.Ext(jsonlFilePath) != ".jsonl" {
		return fmt.Errorf("JSONL file must have a .jsonl extension")
	}
	// verify file exists
	if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
		return fmt.Errorf("JSONL file does not exist: %s", jsonlFilePath)
	}

	// Create a view from the JSONL file
	createViewQuery := fmt.Sprintf(`CREATE VIEW json_view AS SELECT * FROM read_json_auto('%s', format='newline_delimited');`, jsonlFilePath)
	_, err = w.db.Exec(createViewQuery)
	if err != nil {
		return fmt.Errorf("failed to create view from JSONL file: %w", err)
	}
	defer func() {
		// drop the view
		if _, dropErr := w.db.Exec("DROP VIEW json_view;"); dropErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to drop view: %w", dropErr))
		}
	}()

	// we need to generate a different parquet file for each combination of the specified columns:
	// tp_collection, tp_connection, tp_year, tp_month, tp_day
	// Get distinct combinations
	query := `
		SELECT DISTINCT 
			tp_collection, 
			tp_connection, 
			tp_year, 
			tp_month, 
			tp_day 
		FROM json_view
	`
	rows, err := w.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query distinct combinations: %w", err)
	}
	defer rows.Close()

	var errList []error
	// Iterate over each combination and export the data for that combination
	for rows.Next() {
		var (
			tpCollection string
			tpConnection string
			tpYear       string
			tpMonth      string
			tpDay        string
		)
		if err := rows.Scan(&tpCollection, &tpConnection, &tpYear, &tpMonth, &tpDay); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Generate the parquet file path name based on the naming convention
		filePath := w.getParquetFilePath(collectionType, tpCollection, tpConnection, tpYear, tpMonth, tpDay)
		// ensure the folder exists
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			errList = append(errList, fmt.Errorf("failed to create directory %s : %w", filepath.Dir(filePath), err))
			continue
		}

		// Create a query to select data for the specific combination
		exportQuery := fmt.Sprintf(`
			COPY (
				WITH data_group AS (
					SELECT * FROM json_view 
					WHERE tp_collection = '%s'
					AND tp_connection = '%s'
					AND tp_year = '%s'
					AND tp_month = '%s'
					AND tp_day = '%s'
				)
				SELECT * FROM data_group
			) TO '%s' (FORMAT PARQUET);
		`, tpCollection, tpConnection, tpYear, tpMonth, tpDay, filePath)

		_, err = w.db.Exec(exportQuery)
		if err != nil {
			errList = append(errList, fmt.Errorf("failed select JSON data: %w", err))
			continue
		}

		slog.Debug("exported data to parquet", "file", filePath)
	}

	if err := rows.Err(); err != nil {
		errList = append(errList, fmt.Errorf("error converting JSONL file to Parquet: %w", err))
	}
	if len(errList) > 0 {
		return errors.Join(errList...)
	}

	return nil
}

// getParquetFilePath generates the file path for the parquet file based on the naming convention
func (w *fileWorkerBase) getParquetFilePath(collectionType string, tpCollection string, tpConnection string, tpYear string, tpMonth string, tpDay string) string {
	// generate uuid for the filename
	id := uuid.New().String()
	filePath := fmt.Sprintf("%s/tp_collection=%s/tp_connection=%s/tp_year=%s/tp_month=%s/tp_day=%s/%s.parquet",
		collectionType, tpCollection, tpConnection, tpYear, tpMonth, tpDay, id)
	return filepath.Join(w.destDir, filePath)

}
