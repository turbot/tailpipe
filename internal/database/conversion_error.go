package database

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// handleConversionError attempts to handle conversion errors by counting the number of lines in the file.
// if we fail, just return the raw error.
// TODO we need to pass an error prefix into here so we know the context https://github.com/turbot/tailpipe/issues/477
func handleConversionError(err error, paths ...string) error {
	logArgs := []any{
		"error",
		err,
		"path",
		paths,
	}

	// try to count the number of rows in the file
	rows, countErr := countLinesForFiles(paths...)
	if countErr == nil {
		logArgs = append(logArgs, "rows_affected", rows)
	}

	// log error (if this is NOT a memory error
	// memory errors are handles separately and retried
	if !conversionRanOutOfMemory(err) {
		slog.Error("parquet conversion failed", logArgs...)
	}

	// return wrapped error
	return NewConversionError(err, rows, paths...)
}
func countLinesForFiles(filenames ...string) (int64, error) {
	total := 0
	for _, filename := range filenames {
		count, err := countLines(filename)
		if err != nil {
			return 0, fmt.Errorf("failed to count lines in %s: %w", filename, err)
		}
		total += int(count)
	}
	return int64(total), nil
}
func countLines(filename string) (int64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	buf := make([]byte, 64*1024)
	count := 0

	for {
		c, err := file.Read(buf)
		if c > 0 {
			count += bytes.Count(buf[:c], []byte{'\n'})
		}
		if err != nil {
			if err == io.EOF {
				return int64(count), nil
			}
			return 0, err
		}
	}
}

type ConversionError struct {
	SourceFiles  []string
	BaseError    error
	RowsAffected int64
	displayError string
}

func NewConversionError(err error, rowsAffected int64, paths ...string) *ConversionError {
	sourceFiles := make([]string, len(paths))
	for i, path := range paths {
		sourceFiles[i] = filepath.Base(path)
	}
	return &ConversionError{
		SourceFiles:  sourceFiles,
		BaseError:    err,
		RowsAffected: rowsAffected,
		displayError: strings.Split(err.Error(), "\n")[0],
	}
}

func (c *ConversionError) Error() string {
	return fmt.Sprintf("%s: %s", strings.Join(c.SourceFiles, ", "), c.displayError)
}

// Merge adds a second error to the conversion error message.
func (c *ConversionError) Merge(err error) {
	c.BaseError = fmt.Errorf("%w: %w", c.BaseError, err)
}
func (c *ConversionError) Unwrap() error {
	return c.BaseError
}
