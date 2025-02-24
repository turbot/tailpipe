package database

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func Test_isInvalidParquetError(t *testing.T) {
	tests := []struct {
		name               string
		parquetFile        string
		wantFilepath       string
		wantIsParquetError bool
	}{
		{
			name:               "valid parquet file",
			parquetFile:        "testdata/valid.parquet",
			wantFilepath:       "",
			wantIsParquetError: false,
		},
		{
			name:               "too short",
			parquetFile:        "testdata/too_short.parquet",
			wantFilepath:       "testdata/too_short.parquet",
			wantIsParquetError: true,
		},
		{
			name:               "no magic byte",
			parquetFile:        "testdata/no_magic_byte.parquet",
			wantFilepath:       "testdata/no_magic_byte.parquet",
			wantIsParquetError: true,
		},
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parquetFilePath, err := filepath.Abs(tt.parquetFile)
			if err != nil {
				t.Errorf("failed to get absolute path for %s: %v", tt.parquetFile, err)
				return
			}
			query := fmt.Sprintf(`select * from read_parquet('%s')`, parquetFilePath)
			_, err = db.Query(query)

			gotFilepath, isInvalidParquetError := isInvalidParquetError(err)

			wantFilepath := tt.wantFilepath
			if wantFilepath != "" {
				wantFilepath, err = filepath.Abs(wantFilepath)
				if err != nil {
					t.Errorf("failed to get absolute path for %s: %v", tt.wantFilepath, err)
					return
				}

				if gotFilepath != wantFilepath {
					t.Errorf("isInvalidParquetError() got = %v, want %v", gotFilepath, tt.wantFilepath)
				}
				if isInvalidParquetError != tt.wantIsParquetError {
					t.Errorf("isInvalidParquetError() got1 = %v, want1 %v", isInvalidParquetError, tt.wantIsParquetError)
				}
			}
		})
	}
}
