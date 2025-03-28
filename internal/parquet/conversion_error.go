package parquet

import (
	"bytes"
	"io"
	"log/slog"
	"os"
)

// handleConversionError attempts to handle conversion errors by counting the number of lines in the file.
// if we fail, just return the raw error.
func handleConversionError(err error, path string) error {
	// try to count the number of lines in the file
	lines, countErr := countLines(path)
	if countErr != nil {
		// just log and return the error as is
		slog.Error("parquet conversion failed", "error", err, "path", path)
		return err
	}

	// log the error with line count & then return a conversion error
	slog.Error("parquet conversion failed", "error", err, "path", path, "lines", lines)

	return NewConversionError(err, lines)
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
	BaseError    error
	RowsAffected int64
}

func NewConversionError(err error, rowsAffected int64) *ConversionError {
	return &ConversionError{
		BaseError:    err,
		RowsAffected: rowsAffected,
	}
}

func (c *ConversionError) Error() string {
	return c.BaseError.Error()
}
