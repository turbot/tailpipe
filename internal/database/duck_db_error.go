package database

import (
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/repository"

	"github.com/marcboeker/go-duckdb"
)

// executeWithParquetErrorRetry executes a function with retry logic for invalid parquet files.
// When it encounters a DuckDB error indicating an invalid Parquet file (either too short or missing magic bytes),
// it will:
// 1. Check if the error is a known invalid Parquet error pattern
// 2. Rename the invalid file by appending '.invalid' to its name
// 3. Convert the error to an invalidParquetError type
// The function will retry up to 1000 times before giving up, collecting any invalid Parquet errors encountered.
// This high retry count is necessary as large operations may encounter many invalid files during concurrent writes.
func executeWithParquetErrorRetry[T any](fn func() (T, error)) (T, error) {
	var result T
	var partitionErr *partitionError
	var err error
	maxRetries := 1000

	defer func() {
		if partitionErr != nil {
			repository.HandlePartitionErrors(partitionErr.partitionErrors)
		}
	}()

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, err = fn()
		if err == nil {
			// no error - return the result and any partition error encountered
			// (may be nil)
			return result, err
		}

		err = handleDuckDbError(err)
		var i invalidParquetError
		if stderrors.As(err, &i) {
			if partitionErr == nil {
				partitionErr = newPartitionError(i.table, i.partition, i.date)
			} else {
				partitionErr.addError(i.table, i.partition, i.date)
			}
			continue
		}
		// If we get here, it's not a parquet error, so return it
		return result, err
	}

	// If we've exhausted all retries, return the last error we encountered
	return result, err
}

// handleDuckDbError processes DuckDB errors and handles invalid parquet file scenarios.
// When an invalid parquet file is detected (e.g., corrupted or incomplete files), it:
//  1. Renames the invalid file with a .invalid extension
//  2. Returns a specialized error type (invalidParquetError) with details about the affected file
//
// This function is used to gracefully handle cases where parquet files are corrupted
// during concurrent writes or interrupted file operations, allowing the system to
// track and report invalid data without crashing.
func handleDuckDbError(err error) error {
	if fileName, isInvalidParquetError := isInvalidParquetError(err); isInvalidParquetError {
		// rename the parquet file - add a .invalid extension
		// Perform the rename operation

		updatedFilename, renameErr := renameInvalidParquetFile(fileName)
		if renameErr != nil {
			slog.Warn("Failed to rename invalid parquet file", "file", fileName, "error", renameErr)
			// Return the original error if renaming fails
			return err
		}
		return newInvalidParquetError(updatedFilename)
	}
	return err
}

// renameInvalidParquetFile renames an invalid parquet file by appending .invalid to its name.
func renameInvalidParquetFile(filePath string) (string, error) {
	// Create the new file path by appending .invalid
	newPath := filePath + ".invalid"

	// Try to rename the file
	if err := os.Rename(filePath, newPath); err != nil {
		return "", fmt.Errorf("rename %s %s: %w", filePath, newPath, err)
	}
	return newPath, nil
}

// isInvalidParquetError checks if an error is an invalid parquet error and returns the file path
// if it is. This is used to identify errors that occur when trying to read a parquet file that
// is either too short or doesn't have the correct magic bytes, which can happen when the file
// is being written concurrently or file writing is interrupted.
func isInvalidParquetError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	var target *duckdb.Error
	if stderrors.As(err, &target) {
		if target.Type == duckdb.ErrorTypeInvalidInput {
			// Remove the "Invalid Input Error:" prefix if it exists and normalize whitespace
			msg := strings.TrimPrefix(target.Msg, "Invalid Input Error: ")
			msg = strings.ReplaceAll(msg, "\n", " ")
			msg = strings.ReplaceAll(msg, "\r", "")
			slog.Debug("Processing error message", "original", target.Msg, "cleaned", msg)

			// Check for both error message patterns
			if strings.Contains(msg, "No magic bytes found at end of file") ||
				strings.Contains(msg, "too small to be a Parquet file") {
				slog.Debug("Found invalid parquet error pattern")

				// Define regex pattern to match the file path inside single quotes
				re := regexp.MustCompile(`'([^']+)'`)

				// Find the first match
				match := re.FindStringSubmatch(msg)
				slog.Debug("Regex match result", "match", match)
				if len(match) > 1 {
					// Return the extracted file path and true
					return match[1], true
				}
			}
		}
	}
	return "", false
}

type invalidParquetError struct {
	// The filename of the invalid parquet file
	parquetFilePath string
	table           string
	partition       string
	date            time.Time
}

func (e invalidParquetError) Error() string {
	return fmt.Sprintf("invalid parquet file: %s", e.parquetFilePath)
}

// ctor
func newInvalidParquetError(parquetFilePath string) error {
	err := invalidParquetError{
		parquetFilePath: parquetFilePath,
	}

	// Extract table, partition and date from path components
	parts := strings.Split(parquetFilePath, "/")
	for _, part := range parts {
		if strings.HasPrefix(part, "tp_table=") {
			err.table = strings.TrimPrefix(part, "tp_table=")
		} else if strings.HasPrefix(part, "tp_partition=") {
			err.partition = strings.TrimPrefix(part, "tp_partition=")
		} else if strings.HasPrefix(part, "tp_date=") {
			dateString := strings.TrimPrefix(part, "tp_date=")
			date, parseErr := time.Parse("2006-01-02", dateString)
			if parseErr == nil {
				err.date = date
			}
		}
	}
	return err
}

// partitionError is a specialized error type that tracks invalid Parquet files across multiple partitions.
// It's designed to handle scenarios where multiple Parquet files in different partitions are found to be invalid
// during a single operation (e.g., a query that reads from multiple partitions).
//
// The error maintains a map of partition identifiers (in the format "table.partition") to their earliest error times.
// This allows tracking:
// 1. Which partitions had invalid files
// 2. When each partition first encountered an error
// 3. The complete set of affected partitions in a single error
//
// Example error message:
//
//	Invalid parquet files found in partitions:
//	- aws_cloudtrail.cloudtrail (earliest error: 2024-03-20)
//	- aws_cloudtrail.cloudtrail (earliest error: 2024-03-19)
//	- aws_ec2.instance (earliest error: 2024-03-18)
//
// This error type is used by executeWithParquetErrorRetry to collect errors across multiple retry attempts,
// ensuring that we don't lose information about invalid files in different partitions even if we eventually
// succeed in reading some files after retries.
type partitionError struct {
	partitionErrors map[string]time.Time
}

// newPartitionError creates a new partitionError with an initial partition and error time.
// This is typically called when encountering the first invalid Parquet file in a sequence of retries.
//
// Parameters:
//   - table: The table name (e.g., "aws_cloudtrail")
//   - partition: The partition identifier (e.g., "cloudtrail")
//   - errTime: The time when the error was encountered
func newPartitionError(table, partition string, errTime time.Time) *partitionError {
	return &partitionError{
		partitionErrors: map[string]time.Time{
			fmt.Sprintf("%s.%s", table, partition): errTime,
		},
	}
}

func (e *partitionError) Error() string {
	var msg strings.Builder
	msg.WriteString("Invalid parquet files found in partitions:\n")
	// Sort partitions to ensure consistent output
	var partitions []string
	for partition := range e.partitionErrors {
		partitions = append(partitions, partition)
	}
	sort.Strings(partitions)
	for _, partition := range partitions {
		msg.WriteString(fmt.Sprintf("- %s (earliest error: %s)\n", partition, e.partitionErrors[partition].Format("2006-01-02")))
	}
	return msg.String()
}

// addError adds or updates a partition error, keeping the earliest error time.
// If the partition already exists in the error map, the error time is only updated
// if the new error time is earlier than the existing one.
//
// Parameters:
//   - table: The table name (e.g., "aws_cloudtrail")
//   - partition: The partition identifier (e.g., "cloudtrail")
//   - errTime: The time when the error was encountered
func (e *partitionError) addError(table, partition string, errTime time.Time) {
	key := fmt.Sprintf("%s.%s", table, partition)
	if existingTime, exists := e.partitionErrors[key]; !exists || errTime.Before(existingTime) {
		e.partitionErrors[key] = errTime
	}
}
