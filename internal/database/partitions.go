package database

import (
	"context"
	"fmt"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// ListPartitions uses DuckDB to build a list of all partitions for all tables
func ListPartitions(ctx context.Context) ([]string, error) {
	// Hive format is table, partition, index, date

	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is not set")
	}
	// TODO KAI handle no partitions

	// Build DuckDB query to get the names of all partitions underneath data dir
	parquetPath := filepaths.GetParquetFileGlobForTable(dataDir, "*", "")
	query := `SELECT DISTINCT tp_table || '.' || tp_partition FROM read_parquet('` + parquetPath + `', hive_partitioning=true)`

	// Open DuckDB in-memory database
	db, err := NewDuckDb()
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partition string
		if err := rows.Scan(&partition); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		partitions = append(partitions, partition)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return partitions, nil
}
