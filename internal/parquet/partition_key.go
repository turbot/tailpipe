package parquet

import (
	"context"
	"fmt"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/database"
	"time"
)

// partitionKey is used to uniquely identify a a combination of ducklake partition columns:
// tp_table, tp_partition, tp_index, year(tp_timestamp), month(tp_timestamp)
// It also stores the file count for that partition key
type partitionKey struct {
	tpTable     string
	tpPartition string
	tpIndex     string
	year        string // year(tp_timestamp) from partition value
	month       string // month(tp_timestamp) from partition value
	fileCount   int
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file count for each partition key combination
func getPartitionKeysMatchingPattern(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) ([]partitionKey, error) {
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
  fpv4.partition_value
order by file_count desc;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}
	defer rows.Close()

	var partitionKeys []partitionKey
	for rows.Next() {
		var partitionKey partitionKey
		if err := rows.Scan(&partitionKey.tpTable, &partitionKey.tpPartition, &partitionKey.tpIndex, &partitionKey.year, &partitionKey.month, &partitionKey.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}
		// check whether this partition key matches any of the provided patterns
		if PartitionMatchesPatterns(partitionKey.tpTable, partitionKey.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, partitionKey)
		}
	}

	return partitionKeys, nil
}

type partitionKeyRows struct {
	partitionKey partitionKey
	rowCount     int
	fileCount    int
	maxRowId     int
	minTimestamp time.Time
	maxTimestamp time.Time
}

// get partition key statistics: row count, file count  max row id, min and max timestamp
func getPartitionKeyRowCount(ctx context.Context, db *database.DuckDb, partitionKey partitionKey) (*partitionKeyRows, error) {
	var pkr = &partitionKeyRows{}
	pkr.partitionKey = partitionKey

	// Query to get row count, file count, and time range for this partition
	countQuery := fmt.Sprintf(`select count(*), max(rowid), min(tp_timestamp), max(tp_timestamp) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		partitionKey.tpTable)

	if err := db.QueryRowContext(ctx, countQuery,
		partitionKey.tpPartition,
		partitionKey.tpIndex,
		partitionKey.year,
		partitionKey.month).Scan(&pkr.rowCount, &pkr.maxRowId, &pkr.minTimestamp, &pkr.maxTimestamp); err != nil {
		return nil, fmt.Errorf("failed to get row count and time range for partition: %w", err)
	}

	// Get file count for this partition key from DuckLake metadata
	fileCountQuery := fmt.Sprintf(`select count(*) from %s.ducklake_data_file df
		join %s.ducklake_file_partition_value fpv1 on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
		join %s.ducklake_file_partition_value fpv2 on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
		join %s.ducklake_file_partition_value fpv3 on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
		join %s.ducklake_file_partition_value fpv4 on df.data_file_id = fpv4.data_file_id and fpv4.partition_key_index = 3
		join %s.ducklake_table t on df.table_id = t.table_id
		where t.table_name = ? and df.end_snapshot is null
		  and fpv1.partition_value = ? and fpv2.partition_value = ? 
		  and fpv3.partition_value = ? and fpv4.partition_value = ?`,
		constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog,
		constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog)

	if err := db.QueryRowContext(ctx, fileCountQuery,
		partitionKey.tpTable, partitionKey.tpPartition, partitionKey.tpIndex,
		partitionKey.year, partitionKey.month).Scan(&pkr.fileCount); err != nil {
		return nil, fmt.Errorf("failed to get file count for partition: %w", err)
	}

	return pkr, nil
}
