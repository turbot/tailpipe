package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcboeker/go-duckdb"
)

const testDataDir = "testdata"

// copyTestFile copies a test file from the source to the destination directory
func copyTestFile(t *testing.T, src, dstDir string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("failed to read source file %s: %v", src, err)
	}
	dst := filepath.Join(dstDir, filepath.Base(src))
	if err := os.WriteFile(dst, data, 0644); err != nil { //nolint:gosec // test code
		t.Fatalf("failed to write test file %s: %v", dst, err)
	}
	return dst
}

func Test_isInvalidParquetError(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Copy test files to temp directory
	files := []string{
		filepath.Join(testDataDir, "valid.parquet"),
		filepath.Join(testDataDir, "too_short.parquet"),
		filepath.Join(testDataDir, "no_magic_byte.parquet"),
	}

	for _, file := range files {
		copyTestFile(t, file, tmpDir)
	}

	tests := []struct {
		name                string
		parquetFile         string
		wantInvalidFilepath string
		wantIsParquetError  bool
	}{
		{
			name:                "valid parquet file",
			parquetFile:         filepath.Join(tmpDir, "valid.parquet"),
			wantInvalidFilepath: "",
			wantIsParquetError:  false,
		},
		{
			name:                "too short",
			parquetFile:         filepath.Join(tmpDir, "too_short.parquet"),
			wantInvalidFilepath: filepath.Join(tmpDir, "too_short.parquet"),
			wantIsParquetError:  true,
		},
		{
			name:                "no magic byte",
			parquetFile:         filepath.Join(tmpDir, "no_magic_byte.parquet"),
			wantInvalidFilepath: filepath.Join(tmpDir, "no_magic_byte.parquet"),
			wantIsParquetError:  true,
		},
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := fmt.Sprintf(`select * from read_parquet('%s')`, tt.parquetFile) //nolint:gosec // test code
			rows, err := db.Query(query)
			if err == nil {
				_ = rows.Close() //nolint:sqlclosecheck // test code
			}

			// First check if it's an invalid parquet error
			invalidFilepath, isInvalidParquetError := isInvalidParquetError(err)

			if invalidFilepath != tt.wantInvalidFilepath {
				t.Errorf("isInvalidParquetError() got = %v, want %v", invalidFilepath, tt.wantInvalidFilepath)
			}
			if isInvalidParquetError != tt.wantIsParquetError {
				t.Errorf("isInvalidParquetError() got1 = %v, want1 %v", isInvalidParquetError, tt.wantIsParquetError)
			}
		})
	}
}

func Test_executeWithParquetErrorRetry(t *testing.T) {
	tmpDir := t.TempDir()

	// Helper function to create a DuckDB invalid parquet error
	makeInvalidParquetError := func(file string) error {
		return &duckdb.Error{
			Type: duckdb.ErrorTypeInvalidInput,
			Msg:  fmt.Sprintf("Invalid Input Error: No magic bytes found at end of file '%s'", file),
		}
	}

	// Helper function to create a test file with proper path structure
	mkTestFile := func(attempt int) string {
		// Create a path that matches the expected format: tp_table=aws_cloudtrail/tp_partition=cloudtrail/tp_date=2024-03-20/test.parquet.N
		path := filepath.Join(tmpDir, "tp_table=aws_cloudtrail", "tp_partition=cloudtrail", "tp_date=2024-03-20")
		if err := os.MkdirAll(path, 0755); err != nil {
			t.Fatalf("failed to create test directory: %v", err)
		}
		tmpFile := filepath.Join(path, fmt.Sprintf("test.parquet.%d", attempt))
		if err := os.WriteFile(tmpFile, []byte("test data"), 0644); err != nil { //nolint:gosec // test code
			t.Fatalf("failed to create test file: %v", err)
		}
		return tmpFile
	}

	tests := []struct {
		name        string
		errors      []error
		wantErr     bool
		wantErrType error
		wantResult  interface{}
	}{
		{
			name:       "success on first try",
			errors:     []error{nil},
			wantErr:    false,
			wantResult: "success",
		},
		{
			name: "success after multiple retries",
			errors: []error{
				makeInvalidParquetError(mkTestFile(1)),
				makeInvalidParquetError(mkTestFile(2)),
				nil,
			},
			wantErr:    false,
			wantResult: "success",
		},
		{
			name: "multiple invalid parquet files then other error",
			errors: []error{
				makeInvalidParquetError(mkTestFile(1)),
				fmt.Errorf("other error"),
			},
			wantErr:     true,
			wantErrType: fmt.Errorf("other error"),
		},
		{
			name:        "non-parquet error",
			errors:      []error{fmt.Errorf("other error")},
			wantErr:     true,
			wantErrType: fmt.Errorf("other error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var attempt int
			wrappedFn := func() (interface{}, error) {
				attempt++
				if attempt > len(tt.errors) {
					return tt.wantResult, nil
				}
				if tt.errors[attempt-1] == nil {
					return tt.wantResult, nil
				}
				// Create the test file if it's a parquet error
				if _, ok := tt.errors[attempt-1].(*duckdb.Error); ok {
					testFile := mkTestFile(attempt)
					if err := os.WriteFile(testFile, []byte("test data"), 0644); err != nil {
						t.Fatalf("failed to create test file: %v", err)
					}
				}
				return nil, tt.errors[attempt-1]
			}

			result, err := executeWithParquetErrorRetry(wrappedFn)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
				}
				if tt.wantErrType != nil && err.Error() != tt.wantErrType.Error() {
					t.Errorf("got error %v, want %v", err, tt.wantErrType)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != tt.wantResult {
				t.Errorf("got result %v, want %v", result, tt.wantResult)
			}
			if attempt != len(tt.errors) {
				t.Errorf("got %d attempts, want %d", attempt, len(tt.errors))
			}
		})
	}
}

func TestDuckDb_WrapperMethods(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	db, err := NewDuckDb(WithTempDir(tmpDir))
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer db.Close()

	// Test Query
	t.Run("Query", func(t *testing.T) {
		rows, err := db.Query("SELECT 1")
		if err != nil {
			t.Errorf("Query failed: %v", err)
		}
		if rows != nil {
			defer rows.Close()
		}
	})

	// Test QueryContext
	t.Run("QueryContext", func(t *testing.T) {
		ctx := context.Background()
		rows, err := db.QueryContext(ctx, "SELECT 1")
		if err != nil {
			t.Errorf("QueryContext failed: %v", err)
		}
		if rows != nil {
			defer rows.Close()
		}
	})

	// Test QueryRow
	t.Run("QueryRow", func(t *testing.T) {
		row := db.QueryRow("SELECT 1")
		if row == nil {
			t.Error("QueryRow returned nil")
		}
	})

	// Test QueryRowContext
	t.Run("QueryRowContext", func(t *testing.T) {
		ctx := context.Background()
		row := db.QueryRowContext(ctx, "SELECT 1")
		if row == nil {
			t.Error("QueryRowContext returned nil")
		}
	})

	// Test Exec
	t.Run("Exec", func(t *testing.T) {
		result, err := db.Exec("SELECT 1")
		if err != nil {
			t.Errorf("Exec failed: %v", err)
		}
		if result == nil {
			t.Error("Exec returned nil result")
		}
	})

	// Test ExecContext
	t.Run("ExecContext", func(t *testing.T) {
		ctx := context.Background()
		result, err := db.ExecContext(ctx, "SELECT 1")
		if err != nil {
			t.Errorf("ExecContext failed: %v", err)
		}
		if result == nil {
			t.Error("ExecContext returned nil result")
		}
	})
}

// Test_handleDuckDbError verifies that handleDuckDbError correctly handles DuckDB errors by:
// 1. Converting DuckDB invalid Parquet errors to invalidParquetError type
// 2. Renaming invalid Parquet files by appending .invalid extension
// 3. Passing through non-Parquet errors unchanged
func Test_handleDuckDbError(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Copy test files to temp directory - we use real test data files that we know are invalid
	files := []string{
		"testdata/valid.parquet",
		"testdata/too_short.parquet",
		"testdata/no_magic_byte.parquet",
	}

	for _, file := range files {
		copyTestFile(t, file, tmpDir)
	}

	// Define test cases to verify different error scenarios
	tests := []struct {
		name           string
		err            error
		filePath       string      // The file path that should be renamed (if any)
		wantErrType    interface{} // Expected error type after handling (nil means error should be unchanged)
		wantFileExists bool        // Whether we expect the file to be renamed with .invalid extension
	}{
		{
			name: "invalid parquet error - too short",
			err: &duckdb.Error{
				Type: duckdb.ErrorTypeInvalidInput,
				Msg:  fmt.Sprintf("Invalid Input Error: File '%s' too small to be a Parquet file", filepath.Join(tmpDir, "too_short.parquet")),
			},
			filePath:       filepath.Join(tmpDir, "too_short.parquet"),
			wantErrType:    &invalidParquetError{}, // Should convert to invalidParquetError
			wantFileExists: true,                   // Should rename file
		},
		{
			name: "invalid parquet error - no magic bytes",
			err: &duckdb.Error{
				Type: duckdb.ErrorTypeInvalidInput,
				Msg:  fmt.Sprintf("Invalid Input Error: No magic bytes found at end of file '%s'", filepath.Join(tmpDir, "no_magic_byte.parquet")),
			},
			filePath:       filepath.Join(tmpDir, "no_magic_byte.parquet"),
			wantErrType:    &invalidParquetError{}, // Should convert to invalidParquetError
			wantFileExists: true,                   // Should rename file
		},
		{
			name:           "other error",
			err:            fmt.Errorf("some other error"),
			filePath:       "",
			wantErrType:    nil,   // Should not modify the error
			wantFileExists: false, // Should not rename any files
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call handleDuckDbError and check the returned error
			err := handleDuckDbError(tt.err)

			// Verify error type conversion
			if tt.wantErrType != nil {
				if !errors.As(err, tt.wantErrType) {
					t.Errorf("handleDuckDbError() error type = %T, want %T", err, tt.wantErrType)
				}
			} else if err != tt.err {
				t.Errorf("handleDuckDbError() returned different error: got %v, want %v", err, tt.err)
			}

			// Verify file renaming behavior
			if tt.wantFileExists && tt.filePath != "" {
				// Check that file was renamed to .invalid
				if _, err := os.Stat(tt.filePath + ".invalid"); err != nil {
					t.Errorf("file was not renamed to .invalid: %v", err)
				}
				// Check that original file no longer exists
				if _, err := os.Stat(tt.filePath); err == nil {
					t.Error("original file still exists")
				}
			}
		})
	}
}

func Test_partitionError(t *testing.T) {
	// Create some test dates
	date1 := time.Date(2024, 3, 20, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2024, 3, 19, 0, 0, 0, 0, time.UTC)
	date3 := time.Date(2024, 3, 18, 0, 0, 0, 0, time.UTC)

	// Test creating a new partition error
	err := newPartitionError("aws_cloudtrail", "cloudtrail", date1)
	if err == nil {
		t.Fatal("Expected non-nil error")
	}

	// Test adding a later error for the same partition (should not update)
	// Expected: date2 is now the earliest error
	err.addError("aws_cloudtrail", "cloudtrail", date2)
	expectedMsg := fmt.Sprintf("Invalid parquet files found in partitions:\n- aws_cloudtrail.cloudtrail (earliest error: %s)\n", date2.Format("2006-01-02"))
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message:\n%s\nGot:\n%s", expectedMsg, err.Error())
	}

	// Test adding an earlier error for the same partition (should update)
	// Expected: date3 should become the earliest error since it's earlier than date1
	err.addError("aws_cloudtrail", "cloudtrail", date3)
	expectedMsg = fmt.Sprintf("Invalid parquet files found in partitions:\n- aws_cloudtrail.cloudtrail (earliest error: %s)\n", date3.Format("2006-01-02"))
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message:\n%s\nGot:\n%s", expectedMsg, err.Error())
	}

	// Test adding error for a different partition
	// Expected: both partitions should be listed, with their respective earliest error dates
	err.addError("aws_ec2", "instance", date2)
	expectedMsg = fmt.Sprintf("Invalid parquet files found in partitions:\n- aws_cloudtrail.cloudtrail (earliest error: %s)\n- aws_ec2.instance (earliest error: %s)\n",
		date3.Format("2006-01-02"), date2.Format("2006-01-02"))
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message:\n%s\nGot:\n%s", expectedMsg, err.Error())
	}
}
