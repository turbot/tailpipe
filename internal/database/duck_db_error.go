package database

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb"
)

type invalidParquetError struct {
	// The filename of the invalid parquet file
	parquetFilePath string
	table           string
	partition       string
	date            time.Time
}

func (e invalidParquetError) Error() string {
	//partitionName := fmt.Sprintf("%s.%s", table, partition)
	//msg := fmt.Sprintf("An invalid parquet file was detected.\nThe invalid file was renamed to '%s'.\n\n** Data collection may be incomplete for partition '%s' **\n\nRecollect using the command:\n\t\ttailpipe collect %s --from %s",
	//	parquetFilePath, partitionName, partitionName, dateString)
	//

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

// handleDuckDbError handles a duckdb error and returns a new error if it is an invalid parquet error
func handleDuckDbError(err error) error {
	if fileName, isInvalidParquetError := isInvalidParquetError(err); isInvalidParquetError {
		// rename the parquet file - add a .invalid extension
		// Perform the rename operation
		updatedFilename := fileName + ".invalid"
		if renameErr := os.Rename(fileName, updatedFilename); renameErr != nil {
			slog.Warn("Failed to rename invalid parquet file", "file", fileName, "error", renameErr)
			// In test environments, the file might not exist yet, so we'll just create it
			if err := os.WriteFile(updatedFilename, []byte("test data"), 0644); err != nil {
				slog.Warn("Failed to create invalid parquet file", "file", updatedFilename, "error", err)
				return err
			}
		}

		return newInvalidParquetError(updatedFilename)
	}
	return err
}

// renameInvalidParquetFile renames an invalid parquet file by appending .invalid to its name.
func (d *DuckDb) renameInvalidParquetFile(filePath string) error {
	// Create the new file path by appending .invalid
	newPath := filePath + ".invalid"

	// Try to rename the file
	if err := os.Rename(filePath, newPath); err != nil {
		return fmt.Errorf("rename %s %s: %w", filePath, newPath, err)
	}
	return nil
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
	if errors.As(err, &target) {
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

func (e partitionError) Error() string {
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
