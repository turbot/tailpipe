package parquet

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

func (w *Converter) processChunks(chunksToProcess []int32) {
	// note we ALREADY HAVE THE PROCESS LOCK - be sure to release it when we are done
	defer w.processLock.Unlock()

	for len(chunksToProcess) > 0 {
		// build a list of filenames to process
		filenamesToProcess, err := w.chunkNumbersToFilenames(chunksToProcess)
		if err != nil {
			// failed to convert these files - decrement the wait group
			w.wg.Add(len(filenamesToProcess) * -1)

			// TODO #DL re-add error handling
			//  https://github.com/turbot/tailpipe/issues/480
			slog.Error("Error processing chunks", "error", err)
			// store the failed conversion
			//w.failedConversions = append(w.failedConversions, failedConversion{
			//	filenames: filenamesToProcess,
			//	error:     err,
			//},
			//)
			// just carry on
		}

		// execute conversion query for the chunks
		err = w.insertBatchIntoDuckLake(filenamesToProcess)
		if err != nil {
			// TODO #DL re-add error handling
			//  https://github.com/turbot/tailpipe/issues/480

			// NOTE: the wait group will already have been decremented by insertBatchIntoDuckLake
			// so we do not need to decrement it again here

			slog.Error("Error processing chunk", "filenames", filenamesToProcess, "error", err)
			// store the failed conversion
			//w.failedConversions = append(w.failedConversions, failedConversion{
			//	filenames: filenamesToProcess,
			//	error:     err,
			//},
			//)
			// just carry on
		}
		// delete the files after processing
		for _, filename := range filenamesToProcess {
			if err := os.Remove(filename); err != nil {
				slog.Error("Failed to delete file after processing", "file", filename, "error", err)
			}
		}

		// now determine if there are more chunks to process
		w.scheduleLock.Lock()

		// now get next chunks to process
		chunksToProcess = w.getChunksToProcess()

		w.scheduleLock.Unlock()
	}

	// if we get here, we have processed all scheduled chunks (but more may come later
	log.Print("BatchProcessor: all scheduled chunks processed for execution")
}

func (w *Converter) chunkNumbersToFilenames(chunks []int32) ([]string, error) {
	var filenames = make([]string, len(chunks))
	for i, chunkNumber := range chunks {
		// build the source filename
		jsonlFilePath := filepath.Join(w.sourceDir, table.ExecutionIdToJsonlFileName(w.executionId, chunkNumber))
		// verify file exists
		if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
			return nil, NewConversionError(errors.New("file does not exist"), 0, jsonlFilePath)
		}
		// remove single quotes from the file path to avoid issues with SQL queries
		escapedPath := strings.ReplaceAll(jsonlFilePath, "'", "''")
		filenames[i] = escapedPath
	}
	return filenames, nil
}

func (w *Converter) insertBatchIntoDuckLake(filenames []string) error {
	t := time.Now()
	// ensure we signal the converter when we are done
	defer w.wg.Add(len(filenames) * -1)

	// copy the data from the jsonl file to a temp table
	if err := w.copyChunkToTempTable(filenames); err != nil {
		// copyChunkToTempTable will already have called handleSchemaChangeError anf handleConversionError
		return err
	}

	tempTime := time.Now()

	// TODO #DL re-add validation
	//  https://github.com/turbot/tailpipe/issues/479

	// now validate the data
	//if validateRowsError := w.validateRows(jsonlFilePath); validateRowsError != nil {
	//	// if the error is NOT RowValidationError, just return it
	//	if !errors.Is(validateRowsError, &RowValidationError{}) {
	//		return handleConversionError(validateRowsError, jsonlFilePath)
	//	}
	//
	//	// so it IS a row validation error - the invalid rows will have been removed from the temp table
	//	// - process the rest of the chunk
	//	// ensure that we return the row validation error, merged with any other error we receive
	//	defer func() {
	//		if err == nil {
	//			err = validateRowsError
	//		} else {
	//			var conversionError *ConversionError
	//			if errors.As(validateRowsError, &conversionError) {
	//				// we have a conversion error - we need to set the row count to 0
	//				// so we can report the error
	//				conversionError.Merge(err)
	//			}
	//			err = conversionError
	//		}
	//	}()
	//}

	rowCount, err := w.insertIntoDucklake(w.Partition.TableName)
	if err != nil {
		slog.Error("failed to insert into DuckLake table", "table", w.Partition.TableName, "error", err)
		return err
	}

	td := tempTime.Sub(t)
	cd := time.Since(tempTime)
	total := time.Since(t)

	// Update counters and advance to the next batch
	// if we have an error, return it below
	// update the row count
	w.updateRowCount(rowCount)

	slog.Debug("inserted rows into DuckLake table", "chunks", len(filenames), "row count", rowCount, "error", err, "temp time", td.Milliseconds(), "conversion time", cd.Milliseconds(), "total time ", total.Milliseconds())
	return nil
}

func (w *Converter) copyChunkToTempTable(jsonlFilePaths []string) error {
	var queryBuilder strings.Builder

	// Create SQL array of file paths
	var fileSQL string
	if len(jsonlFilePaths) == 1 {

		fileSQL = fmt.Sprintf("'%s'", jsonlFilePaths[0])
	} else {
		// For multiple files, create a properly quoted array
		var quotedPaths []string
		for _, jsonFilePath := range jsonlFilePaths {
			quotedPaths = append(quotedPaths, fmt.Sprintf("'%s'", jsonFilePath))
		}
		fileSQL = "[" + strings.Join(quotedPaths, ", ") + "]"
	}

	// render the read JSON query with the jsonl file path
	// - this build a select clause which selects the required data from the JSONL file (with columns types specified)
	selectQuery := fmt.Sprintf(w.readJsonQueryFormat, fileSQL)

	// Step: Prepare the temp table from JSONL input
	//
	// - Drop the temp table if it exists
	// - Create a new temp table by reading from the JSONL file
	// - Add a row ID (row_number) for stable ordering and chunking
	// - Wrap the original select query to allow dot-notation filtering on nested structs later
	// - Sort the data by partition key columns (only tp_index, tp_date - there will only be a single table and partition)
	// so that full partitions can be selected using only row offsets (because partitions are stored contiguously)
	queryBuilder.WriteString(fmt.Sprintf(`
drop table if exists temp_data;

create temp table temp_data as
  %s
`, selectQuery))

	_, err := w.db.Exec(queryBuilder.String())
	if err != nil {
		// if the error is a schema change error, determine whether the schema of these chunk is
		// different to the inferred schema (pass the first json file)
		return w.handleSchemaChangeError(err, jsonlFilePaths[0])
	}
	return nil
}

// insertIntoDucklakeForBatch writes a batch of rows from the temp_data table to the specified target DuckDB table.
//
// It selects rows based on rowid, using the provided startRowId and rowCount to control the range:
// - Rows with rowid > startRowId and rowid <= (startRowId + rowCount) are selected.
//
// This approach allows for efficient batching from the temporary table into the final destination table.
//
// To prevent schema mismatches, it explicitly lists columns in the INSERT statement based on the conversion schema.
//
// Returns the number of rows inserted and any error encountered.
func (w *Converter) insertIntoDucklake(targetTable string) (int64, error) {
	// quote the table name
	targetTable = fmt.Sprintf(`"%s"`, targetTable)

	// Build the final INSERT INTO ... SELECT statement using the fully qualified table name.
	columns := w.conversionSchema.ColumnString
	insertQuery := fmt.Sprintf(`
		insert into %s (%s)
			select %s from temp_data
	`, targetTable, columns, columns)

	// Execute the insert statement
	result, err := w.db.Exec(insertQuery)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to insert data into DuckLake table db %p", w.db.DB), "table", targetTable, "error", err, "db", w.db.DB)
		// It's helpful to wrap the error with context about what failed.
		return 0, fmt.Errorf("failed to insert data into %s: %w", targetTable, err)
	}

	// Get the number of rows that were actually inserted.
	insertedRowCount, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get number of affected rows: %w", err)
	}

	slog.Debug("inserted rows into ducklake table", "table", targetTable, "count", insertedRowCount)

	return insertedRowCount, nil
}

// handleSchemaChangeError determines if the error is because the schema of this chunk is different to the inferred schema
// infer the schema of this chunk and compare - if they are different, return that in an error
func (w *Converter) handleSchemaChangeError(err error, jsonlFilePath string) error {
	schemaChangeErr := w.detectSchemaChange(jsonlFilePath)
	if schemaChangeErr != nil {
		// if the error returned from detectSchemaChange is a SchemaChangeError, return that instead of the original error
		var e = &SchemaChangeError{}
		if errors.As(schemaChangeErr, &e) {
			// update err and fall through to handleConversionError - this wraps the error with additional row count info
			err = e
		}
	}

	// just return the original error, wrapped with the row count
	return handleConversionError(err, jsonlFilePath)
}

// conversionRanOutOfMemory checks if the error is an out-of-memory error from DuckDB
func conversionRanOutOfMemory(err error) bool {
	var duckDBErr = &duckdb.Error{}
	if errors.As(err, &duckDBErr) {
		return duckDBErr.Type == duckdb.ErrorTypeOutOfMemory
	}
	return false
}
