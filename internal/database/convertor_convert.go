package database

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"

	"github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

// process all available chunks
// this is called when a chunk is added but will continue processing any further chunks added while we were processing
func (w *Converter) processAllChunks() {
	// note we ALREADY HAVE THE PROCESS LOCK - be sure to release it when we are done
	defer w.processLock.Unlock()

	// so we have the process lock AND the schedule lock
	// move the scheduled chunks to the chunks to process
	// (scheduledChunks may be empty, in which case we will break out of the loop)
	chunksToProcess := w.getChunksToProcess()
	for len(chunksToProcess) > 0 {
		err := w.processChunks(chunksToProcess)
		if err != nil {
			slog.Error("Error processing chunks", "error", err)
			// call add job errors and carry on
			w.addJobErrors(err)
		}
		//- get next batch of chunks
		chunksToProcess = w.getChunksToProcess()
	}

	// if we get here, we have processed all scheduled chunks (but more may come later
	log.Print("BatchProcessor: all scheduled chunks processed for execution")
}

// process a batch of chunks
// Note whether successful of not, this decrements w.wg by the chunk count on return
func (w *Converter) processChunks(chunksToProcess []int32) error {
	// decrement the wait group by the number of chunks processed
	defer func() {
		w.wg.Add(len(chunksToProcess) * -1)
	}()

	// build a list of filenames to process
	filenamesToProcess, err := w.chunkNumbersToFilenames(chunksToProcess)
	if err != nil {
		slog.Error("chunkNumbersToFilenames failed")
		// chunkNumbersToFilenames returns a conversionError
		return err
	}

	// execute conversion query for the chunks
	// (insertBatchIntoDuckLake will return a coinversionError)
	err = w.insertBatchIntoDuckLake(filenamesToProcess)
	// delete the files after processing (successful or otherwise) - we will just return err
	for _, filename := range filenamesToProcess {
		if deleteErr := os.Remove(filename); deleteErr != nil {
			slog.Error("Failed to delete file after processing", "file", filename, "error", err)
			// give conversion error precedence
			if err == nil {
				err = deleteErr
			}
		}
	}
	// return error (if any)
	return err
}

func (w *Converter) chunkNumbersToFilenames(chunks []int32) ([]string, error) {
	var filenames = make([]string, len(chunks))
	var missingFiles []string
	for i, chunkNumber := range chunks {
		// build the source filename
		jsonlFilePath := filepath.Join(w.sourceDir, table.ExecutionIdToJsonlFileName(w.executionId, chunkNumber))
		// verify file exists
		if _, err := os.Stat(jsonlFilePath); os.IsNotExist(err) {
			missingFiles = append(missingFiles, jsonlFilePath)
		}
		// remove single quotes from the file path to avoid issues with SQL queries
		escapedPath := strings.ReplaceAll(jsonlFilePath, "'", "''")
		filenames[i] = escapedPath
	}
	if len(missingFiles) > 0 {
		// raise conversion error for the missing files - we do now know the row count so pass zero
		return filenames, NewConversionError(fmt.Errorf("%s not found",
			utils.Pluralize("file", len(missingFiles))),
			0,
			missingFiles...)

	}
	return filenames, nil
}

func (w *Converter) insertBatchIntoDuckLake(filenames []string) (err error) {
	t := time.Now()

	// copy the data from the jsonl file to a temp table
	if err := w.copyChunkToTempTable(filenames); err != nil {
		// copyChunkToTempTable will already have called handleSchemaChangeError anf handleConversionError
		return err
	}

	tempTime := time.Now()

	// now validate the data
	validateRowsError := w.validateRows(filenames)
	if validateRowsError != nil {
		// if the error is NOT RowValidationError, just return it
		// (if it is a validation error, we have special handling)
		if !errors.Is(validateRowsError, &RowValidationError{}) {
			return validateRowsError
		}

		// so it IS a row validation error - the invalid rows will have been removed from the temp table
		// - process the rest of the chunk
		// ensure that we return the row validation error, merged with any other error we receive
		defer func() {
			if err == nil {
				err = validateRowsError
			} else {
				// so we have an error (aside from the any  validation error)
				// convert the validation error to a conversion error (which will be wrapping the validation error
				var conversionError *ConversionError
				// we expect this will always pass
				if errors.As(validateRowsError, &conversionError) {
					conversionError.Merge(err)
				}
				err = conversionError
			}
		}()
	}

	slog.Debug("about to insert rows into ducklake table")

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

	// Check for empty file paths
	if len(jsonlFilePaths) == 0 {
		return fmt.Errorf("no file paths provided")
	}

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
	// - Create a new temp table by executing the dselect query
	queryBuilder.WriteString(fmt.Sprintf(`
drop table if exists temp_data;

create temp table temp_data as
  %s
`, selectQuery))

	_, err := w.db.Exec(queryBuilder.String())
	// TODO KAI think about schema change
	//if err != nil {
	//	// if the error is a schema change error, determine whether the schema of these chunk is
	//	// different to the inferred schema (pass the first json file)
	//	return w.handleSchemaChangeError(err, jsonlFilePaths...)
	//}

	return err
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

	return insertedRowCount, nil
}

// TODO kai think about ducklake schema change detection
//
//// handleSchemaChangeError determines if the error is because the schema of this chunk is different to the inferred schema
//// infer the schema of this chunk and compare - if they are different, return that in an error
//func (w *Converter) handleSchemaChangeError(err error, jsonlFilePath ...string) error {
//	schemaChangeErr := w.detectSchemaChange(jsonlFilePath)
//	if schemaChangeErr != nil {
//		// if the error returned from detectSchemaChange is a SchemaChangeError, return that instead of the original error
//		var e = &SchemaChangeError{}
//		if errors.As(schemaChangeErr, &e) {
//			// update err and fall through to handleConversionError - this wraps the error with additional row count info
//			err = e
//		}
//	}
//
//	// just return the original error, wrapped with the row count
//}

// conversionRanOutOfMemory checks if the error is an out-of-memory error from DuckDB
func conversionRanOutOfMemory(err error) bool {
	var duckDBErr = &duckdb.Error{}
	if errors.As(err, &duckDBErr) {
		return duckDBErr.Type == duckdb.ErrorTypeOutOfMemory
	}
	return false
}
