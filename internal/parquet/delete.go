package parquet

import (
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"os"
	"strings"
	"time"
)

func DeleteParquetFiles(partition *config.Partition, from time.Time) (int, error) {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	fileGlob := database.GetParquetFileGlobForPartition(dataDir, partition.TableName, partition.ShortName, "")

	// TODO verify for SQL injection - c an we use params
	query := fmt.Sprintf(`
    SELECT 
    DISTINCT '%s/tp_table=' || tp_table || '/tp_partition=' || tp_partition || '/tp_index=' || tp_index || '/tp_date=' || tp_date AS hive_path
    FROM read_parquet('%s', hive_partitioning=true)
    WHERE tp_partition = '%s';
`, dataDir, fileGlob, partition.ShortName)

	if !from.IsZero() {
		query += fmt.Sprintf(`
    AND tp_date >= '%s'`, from)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		// is this an error because there are no files?
		if strings.HasPrefix(err.Error(), "IO Error: No files found") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query parquet folder names: %w", err)
	}
	defer rows.Close()

	var folders []string
	// Iterate over the results
	for rows.Next() {
		var folder string
		if err := rows.Scan(&folder); err != nil {
			return 0, fmt.Errorf("failed to scan parquet folder name: %w", err)
		}
		folders = append(folders, folder)
	}

	var errors = make(map[string]error)
	for _, folder := range folders {
		if err := os.RemoveAll(folder); err != nil {
			errors[folder] = err
		}
	}

	return len(folders), nil
}
