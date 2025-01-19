package parquet

import (
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"os"
	"time"
)

func DeleteParquetFiles(partition *config.Partition, from time.Time) error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()

	fileGlob := database.GetParquetFileGlob(dataDir, partition.TableName, "")

	// TODO verify for SQL injection - c an we use params
	query := fmt.Sprintf(`
    SELECT DISTINCT regexp_replace(filename, '/[^/]+$', '') AS folder_path
    FROM read_parquet('%s',filename=true)
    WHERE tp_partition = '%s'
      AND tp_date > '%s';
`, fileGlob, partition.ShortName, from)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query parquet folder names: %w", err)
	}
	defer rows.Close()

	var folders []string
	// Iterate over the results
	for rows.Next() {
		var date string
		if err := rows.Scan(&date); err != nil {
			return fmt.Errorf("failed to scan parquet folder name: %w", err)
		}
		folders = append(folders, date)
	}

	var errors = make(map[string]error)
	for _, folder := range folders {
		if err := os.RemoveAll(folder); err != nil {
			errors[folder] = err
		}
	}

	return nil
}
