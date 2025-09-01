package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/constants"

	"github.com/turbot/tailpipe/internal/database"
)

// partitionKey is used to uniquely identify a a combination of ducklake partition columns:
// tp_table, tp_partition, tp_index, year(tp_timestamp), month(tp_timestamp)
// It also stores the file and row stats for that partition key
type partitionKey struct {
	tpTable     string
	tpPartition string
	tpIndex     string
	year        string // year(tp_timestamp) from partition value
	month       string // month(tp_timestamp) from partition value
	fileCount   int    // number of files for this partition key
	stats       partitionKeyStats
}

// get partition key statistics: row count, file count  max row id, min and max timestamp
func (p *partitionKey) getStats(ctx context.Context, db *database.DuckDb) error {
	stats, err := newPartitionKeyStats(ctx, db, p)
	if err != nil {
		return err
	}

	p.stats = *stats
	return nil
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file and row stats for each partition key combination
func getPartitionKeysMatchingPattern(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) ([]*partitionKey, error) {
	// This query joins the DuckLake metadata tables to get partition key combinations:
	// - ducklake_data_file: contains file metadata and links to tables
	// - ducklake_file_partition_value: contains partition values for each file
	// - ducklake_table: contains table names
	//
	// The partition key structure is:
	// - fpv1 (index 0): tp_partition (e.g., "2024-07")
	// - fpv2 (index 1): tp_index (e.g., "index1")
	// - fpv3 (index 2): year(tp_timestamp) (e.g., "2024")
	// - fpv4 (index 3): month(tp_timestamp) (e.g., "7")
	//
	// We group by these partition keys and count files per combination,
	// filtering for active files (end_snapshot is null)
	// NOTE: Assumes partitions are defined in order: tp_partition (0), tp_index (1), year(tp_timestamp) (2), month(tp_timestamp) (3)
	query := `select 
  t.table_name as tp_table,
  fpv1.partition_value as tp_partition,
  fpv2.partition_value as tp_index,
  fpv3.partition_value as year,
  fpv4.partition_value as month,
  count(*) as file_count
from __ducklake_metadata_tailpipe_ducklake.ducklake_data_file df
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv1
  on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv2
  on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv3
  on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv4
  on df.data_file_id = fpv4.data_file_id and fpv4.partition_key_index = 3
join __ducklake_metadata_tailpipe_ducklake.ducklake_table t
  on df.table_id = t.table_id
where df.end_snapshot is null
group by 
  t.table_name,
  fpv1.partition_value,
  fpv2.partition_value,
  fpv3.partition_value,
  fpv4.partition_value;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}
	defer rows.Close()

	var partitionKeys []*partitionKey
	for rows.Next() {
		var partitionKey = &partitionKey{}

		if err := rows.Scan(&partitionKey.tpTable, &partitionKey.tpPartition, &partitionKey.tpIndex, &partitionKey.year, &partitionKey.month, &partitionKey.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}
		// check whether this partition key matches any of the provided patterns and whether there are any files
		if partitionKey.fileCount > 0 && PartitionMatchesPatterns(partitionKey.tpTable, partitionKey.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, partitionKey)
		}
	}

	// now get the stats for each partition key
	for _, pk := range partitionKeys {
		// populate the stats for the key
		if err := pk.getStats(ctx, db); err != nil {
			return nil, fmt.Errorf("failed to get stats for partition key %v: %w", pk, err)
		}
	}

	return partitionKeys, nil
}

// getFileCountForPartitionKeys returns the count of parquet files for the provided partition keys
func getFileCountForPartitionKeys(ctx context.Context, db *database.DuckDb, partitionLKeys []*partitionKey) (int, error) {
	slog.Info("Getting DuckLake parquet file count for specific partition keys")

	if len(partitionLKeys) == 0 {
		return 0, nil
	}

	// Build a query to count files only for the specified partition keys
	query := fmt.Sprintf(`select count(*) from %s.ducklake_data_file df
		join %s.ducklake_file_partition_value fpv1 on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
		join %s.ducklake_file_partition_value fpv2 on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
		join %s.ducklake_file_partition_value fpv3 on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
		join %s.ducklake_file_partition_value fpv4 on df.data_file_id = fpv4.data_file_id and fpv4.partition_key_index = 3
		where df.end_snapshot is null
		  and (fpv1.partition_value, fpv2.partition_value, fpv3.partition_value, fpv4.partition_value) in (`,
		constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog)

	// Build the IN clause with all partition key combinations
	var values []string
	for _, pk := range partitionLKeys {
		value := fmt.Sprintf("('%s', '%s', '%s', '%s')", pk.tpPartition, pk.tpIndex, pk.year, pk.month)
		values = append(values, value)
	}

	query += strings.Join(values, ", ") + ")"

	var count int
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		if ctx.Err() != nil {
			return 0, err
		}
		return 0, fmt.Errorf("failed to get parquet file count for partition keys: %w", err)
	}
	slog.Info("DuckLake parquet file count retrieved for partition keys", "count", count, "partition_keys", len(partitionLKeys))
	return count, nil
}

type partitionKeyStats struct {
	rowCount     int64
	maxRowId     int64
	minTimestamp time.Time
	maxTimestamp time.Time
}

func newPartitionKeyStats(ctx context.Context, db *database.DuckDb, p *partitionKey) (*partitionKeyStats, error) {
	var stats = &partitionKeyStats{}

	// Query to get row count and time range for this partition
	countQuery := fmt.Sprintf(`select count(*), max(rowid), min(tp_timestamp), max(tp_timestamp) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		p.tpTable)

	err := db.QueryRowContext(ctx, countQuery,
		p.tpPartition,
		p.tpIndex,
		p.year,
		p.month).Scan(&stats.rowCount, &stats.maxRowId, &stats.minTimestamp, &stats.maxTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count and time range for partition: %w", err)
	}

	return stats, nil
}

// disorderMetrics represents the fragmentation level of data for a partition key
type disorderMetrics struct {
	totalFiles       int // total number of files for this partition key
	overlappingFiles int // number of files with overlapping timestamp ranges
}

// getDisorderMetrics calculates the disorder level of data for a partition key
func (p *partitionKey) getDisorderMetrics(ctx context.Context, db *database.DuckDb) (*disorderMetrics, error) {
	// Single query to get files and their timestamp ranges for this partition key
	query := `select 
		df.data_file_id,
		cast(fcs.min_value as timestamp) as min_timestamp,
		cast(fcs.max_value as timestamp) as max_timestamp
	from __ducklake_metadata_tailpipe_ducklake.ducklake_data_file df
	join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv1
	  on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
	join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv2
	  on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
	join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv3
	  on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
	join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv4
	  on df.data_file_id = fpv4.data_file_id and fpv4.partition_key_index = 3
	join __ducklake_metadata_tailpipe_ducklake.ducklake_table t
	  on df.table_id = t.table_id
	join __ducklake_metadata_tailpipe_ducklake.ducklake_file_column_statistics fcs
	  on df.data_file_id = fcs.data_file_id
	join __ducklake_metadata_tailpipe_ducklake.ducklake_column c
	  on fcs.column_id = c.column_id
	where t.table_name = ?
	  and fpv1.partition_value = ?
	  and fpv2.partition_value = ?
	  and fpv3.partition_value = ?
	  and fpv4.partition_value = ?
	  and c.column_name = 'tp_timestamp'
	  and df.end_snapshot is null
	  and c.end_snapshot is null
	order by df.data_file_id`

	rows, err := db.QueryContext(ctx, query, p.tpTable, p.tpPartition, p.tpIndex, p.year, p.month)
	if err != nil {
		return nil, fmt.Errorf("failed to get file timestamp ranges: %w", err)
	}
	defer rows.Close()

	var fileRanges []struct{ min, max time.Time }
	for rows.Next() {
		var fileID int64
		var minTime, maxTime time.Time
		if err := rows.Scan(&fileID, &minTime, &maxTime); err != nil {
			return nil, fmt.Errorf("failed to scan file range: %w", err)
		}
		fileRanges = append(fileRanges, struct{ min, max time.Time }{minTime, maxTime})
	}

	totalFiles := len(fileRanges)
	if totalFiles <= 1 {
		return &disorderMetrics{totalFiles: totalFiles, overlappingFiles: 0}, nil
	}

	// Count overlapping pairs
	overlappingCount := 0
	for i := 0; i < len(fileRanges); i++ {
		for j := i + 1; j < len(fileRanges); j++ {
			file1 := fileRanges[i]
			file2 := fileRanges[j]

			// Check if ranges overlap
			if !(file1.max.Before(file2.min) || file2.max.Before(file1.min)) {
				overlappingCount++
			}
		}
	}

	return &disorderMetrics{
		totalFiles:       totalFiles,
		overlappingFiles: overlappingCount,
	}, nil
}
