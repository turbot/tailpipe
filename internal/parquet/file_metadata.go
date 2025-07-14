package parquet

import (
	"context"
	"fmt"

	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/database"
)

// FileMetadata represents the result of a file metadata query
type FileMetadata struct {
	FileSize  int64
	FileCount int64
	RowCount  int64
}

// TableExists checks if a table exists in the DuckLake metadata tables
func TableExists(ctx context.Context, tableName string, db *database.DuckDb) (bool, error) {
	query := fmt.Sprintf(`select count(*) from %s.ducklake_table where table_name = ?`, constants.DuckLakeMetadataCatalog)

	var count int64
	err := db.QueryRowContext(ctx, query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("unable to check if table %s exists: %w", tableName, err)
	}

	return count > 0, nil
}

// GetTableFileMetadata gets file metadata for a specific table from DuckLake metadata tables
func GetTableFileMetadata(ctx context.Context, tableName string, db *database.DuckDb) (*FileMetadata, error) {
	// first see if the table exists
	exists, err := TableExists(ctx, tableName, db)
	if err != nil {
		return nil, fmt.Errorf("unable to check if table %s exists: %w", tableName, err)
	}
	if !exists {
		// leave everything at zero
		return &FileMetadata{}, nil
	}

	query := fmt.Sprintf(`select
    sum(f.file_size_bytes) as total_size,
    count(*) as file_count,
    sum(f.record_count) as row_count
from %s.ducklake_data_file f
    join %s.ducklake_partition_info p on f.partition_id = p.partition_id
    join %s.ducklake_table tp on p.table_id = tp.table_id
where tp.table_name = ? and f.end_snapshot is null`,
		constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog)

	var totalSize, fileCount, rowCount int64
	err = db.QueryRowContext(ctx, query, tableName).Scan(&totalSize, &fileCount, &rowCount)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain file metadata for table %s: %w", tableName, err)
	}

	return &FileMetadata{
		FileSize:  totalSize,
		FileCount: fileCount,
		RowCount:  rowCount,
	}, nil
}

// GetPartitionFileMetadata gets file metadata for a specific partition from DuckLake metadata tables
func GetPartitionFileMetadata(ctx context.Context, tableName, partitionName string, db *database.DuckDb) (*FileMetadata, error) {
	// first see if the table exists
	exists, err := TableExists(ctx, tableName, db)
	if err != nil {
		return nil, fmt.Errorf("unable to check if table %s exists: %w", tableName, err)
	}
	if !exists {
		// leave everything at zero
		return &FileMetadata{}, nil
	}

	query := fmt.Sprintf(`select
	coalesce(sum(f.file_size_bytes), 0) as total_size,
	coalesce(count(*), 0) as file_count,
	coalesce(sum(f.record_count), 0) as row_count
from %s.ducklake_data_file f
	join %s.ducklake_file_partition_value fpv on f.data_file_id = fpv.data_file_id
	join %s.ducklake_table tp on fpv.table_id = tp.table_id
where tp.table_name = ? and fpv.partition_value = ? and f.end_snapshot is null`,
		constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog)

	var totalSize, fileCount, rowCount int64
	err = db.QueryRowContext(ctx, query, tableName, partitionName).Scan(&totalSize, &fileCount, &rowCount)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain file metadata for partition %s.%s: %w", tableName, partitionName, err)
	}

	return &FileMetadata{
		FileSize:  totalSize,
		FileCount: fileCount,
		RowCount:  rowCount,
	}, nil
}
