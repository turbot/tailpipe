package database

import (
	"errors"
	"github.com/marcboeker/go-duckdb"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

func handleDuckDbError(err error) error {
	if fileName, isInvalidParquetError := isInvalidParquetError(err); isInvalidParquetError {
		// update the invalid parquet file
		invalidParquet, loadErr := LoadInvalidParquet()
		if loadErr != nil {
			// if we cannot load the invalid parquet file, skip the rename and just return the original error
			return err
		}
		// rename the parquet file - add a .invalid extension
		// Perform the rename operation
		updatedFilename := fileName + ".invalid"

		msg, err := GenerateInvalidParquetMessage(updatedFilename)
		if err != nil {
			// we fail to generate the error - this may be because the filename is not in the expected hive format
			// do not rename, just return the original error
			return err
		}

		if renameErr := os.Rename(fileName, updatedFilename); renameErr != nil {
			slog.Warn("Failed to rename invalid parquet file", "file", fileName, "error", renameErr)
			// just return the original error
			return err
		}
		// add the file to the list of invalid parquet files (this handles deduping)
		invalidParquet.AddFile(fileName)
		err = invalidParquet.Save()
		if err != nil {
			slog.Warn("Failed to save invalid parquet file", "file", fileName, "error", err)
			// rename the file back to the original name
			if renameErr := os.Rename(updatedFilename, fileName); renameErr != nil {
				slog.Warn("Failed to revert rename of invalid parquet file", "file", updatedFilename, "error", renameErr)
			}
			// just return the original error
			return err
		}

		return errors.New(msg)
	}
	return err
}

func isInvalidParquetError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var target *duckdb.Error
	if errors.As(err, &target) {
		if target.Type == duckdb.ErrorTypeInvalidInput &&
			strings.HasPrefix(target.Msg, "Invalid Input Error: No magic bytes found at end of file '") ||
			strings.HasPrefix(target.Msg, "Invalid Input Error: File '") {

			// Define regex pattern to match the file path inside single quotes
			re := regexp.MustCompile(`'([^']+)'`)

			// Find the first match
			match := re.FindStringSubmatch(target.Msg)
			if len(match) > 1 {
				// Return the extracted file path and true
				return match[1], true
			}
		}
	}
	return "", false
}
