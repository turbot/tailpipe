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

type partitionFileCount struct {
	tpTable     string
	tpPartition string
	tpIndex     string
	tpDate      time.Time
	fileCount   int
}

func CompactDataFilesManual(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) (*CompactionStatus, error) {
	var status = NewCompactionStatus()

	// get a list of partition key combinations which match any of the patterns
	// partitionKeys is a list of partitionFileCount structs
	partitionKeys, err := getPartitionKeysMatchingPattern(ctx, db, patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}

	// fail early if no matches
	if len(partitionKeys) == 0 {
		slog.Info("No matching partitions found for compaction")
		return status, nil
	}

	// now for each partition key which has more than on parquet file, compact the files by creating a new snapshot
	for _, partitionKey := range partitionKeys {
		if partitionKey.fileCount <= 1 {
			// if the file count is 1 or less, we do not need to compact
			// no need to compact, just increment the uncompacted count
			status.Uncompacted += partitionKey.fileCount
			continue
		}

		slog.Debug("Compacting partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"tp_date", partitionKey.tpDate,
			"file_count", partitionKey.fileCount,
		)
		// increment the source file count by the file count for this partition key
		status.Source += partitionKey.fileCount
		if err := compactAndOrderPartitionEntries(ctx, db, partitionKey); err != nil {
			slog.Error("Failed to compact and order partition entries",
				"tp_table", partitionKey.tpTable,
				"tp_partition", partitionKey.tpPartition,
				"tp_index", partitionKey.tpIndex,
				"tp_date", partitionKey.tpDate,
				"file_count", partitionKey.fileCount,
				"error", err,
			)
			return nil, err
		}

		slog.Info("Compacted and ordered partition entries",
			"tp_table", partitionKey.tpTable,
			"tp_partition", partitionKey.tpPartition,
			"tp_index", partitionKey.tpIndex,
			"tp_date", partitionKey.tpDate,
			"input_files", partitionKey.fileCount,
			"output_files", 1,
		)
		// increment the destination file count by 1 for each partition key
		status.Dest++
	}
	return status, nil
}

func compactAndOrderPartitionEntries(ctx context.Context, db *database.DuckDb, partitionKey partitionFileCount) error {
	// Create ordered snapshot for this partition combination
	// Only process partitions that have multiple files (fileCount > 1)
	snapshotQuery := fmt.Sprintf(`call ducklake.create_snapshot(
		'%s', '%s',
		snapshot_query => $$
			SELECT * FROM "%s"
			WHERE tp_partition = '%s' 
			  AND tp_index = '%s'
			  AND tp_date = '%s'
			ORDER BY tp_timestamp
		$$
	)`,
		SafeIdentifier(constants.DuckLakeCatalog),
		SafeIdentifier(partitionKey.tpTable),
		SafeIdentifier(partitionKey.tpTable),
		EscapeLiteral(partitionKey.tpPartition),
		EscapeLiteral(partitionKey.tpIndex),
		partitionKey.tpDate.Format("2006-01-02"),
	)

	if _, err := db.ExecContext(ctx, snapshotQuery); err != nil {
		return fmt.Errorf("failed to compact and order partition entries for tp_table %s, tp_partition %s, tp_index %s, date %s: %w",
			partitionKey.tpTable, partitionKey.tpPartition, partitionKey.tpIndex, partitionKey.tpDate.Format("2006-01-02"), err)
	}
	return nil
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file count for each partition key combination
func getPartitionKeysMatchingPattern(ctx context.Context, db *database.DuckDb, patterns []PartitionPattern) ([]partitionFileCount, error) {
	// This query joins the DuckLake metadata tables to get partition key combinations:
	// - ducklake_data_file: contains file metadata and links to tables
	// - ducklake_file_partition_value: contains partition values for each file
	// - ducklake_table: contains table names
	//
	// The partition key structure is:
	// - fpv1 (index 0): tp_partition (e.g., "2024-07")
	// - fpv2 (index 1): tp_index (e.g., "index1")
	// - fpv3 (index 2): tp_date
	//
	// We group by these partition keys and count files per combination,
	// filtering for active files (end_snapshot is null)
	// NOTE: Assumes partitions are defined in order: tp_partition (0), tp_index (1), tp_date (2)
	query := `select 
  t.table_name as tp_table,
  fpv1.partition_value as tp_partition,
  fpv2.partition_value as tp_index,
  fpv3.partition_value as tp_date,
  count(*) as file_count
from __ducklake_metadata_tailpipe_ducklake.ducklake_data_file df
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv1
  on df.data_file_id = fpv1.data_file_id and fpv1.partition_key_index = 0
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv2
  on df.data_file_id = fpv2.data_file_id and fpv2.partition_key_index = 1
join __ducklake_metadata_tailpipe_ducklake.ducklake_file_partition_value fpv3
  on df.data_file_id = fpv3.data_file_id and fpv3.partition_key_index = 2
join __ducklake_metadata_tailpipe_ducklake.ducklake_table t
  on df.table_id = t.table_id
where df.end_snapshot is null
group by 
  t.table_name,
  fpv1.partition_value,
  fpv2.partition_value,
  fpv3.partition_value
order by file_count desc;`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition keys requiring compaction: %w", err)
	}
	defer rows.Close()

	var partitionKeys []partitionFileCount
	for rows.Next() {
		var partitionKey partitionFileCount
		if err := rows.Scan(&partitionKey.tpTable, &partitionKey.tpPartition, &partitionKey.tpIndex, &partitionKey.tpDate, &partitionKey.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}
		// check whether this partition key matches any of the provided patterns
		if PartitionMatchesPatterns(partitionKey.tpTable, partitionKey.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, partitionKey)
		}
	}
	return partitionKeys, nil
}

// SafeIdentifier ensures that SQL identifiers (like table or column names)
// are safely quoted using double quotes and escaped appropriately.
//
// For example:
//
//	input:  my_table         → output:  "my_table"
//	input:  some"col         → output:  "some""col"
//	input:  select           → output:  "select"    (reserved keyword)
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func SafeIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return `"` + escaped + `"`
}

// EscapeLiteral safely escapes SQL string literals for use in WHERE clauses,
// INSERTs, etc. It wraps the string in single quotes and escapes any internal
// single quotes by doubling them.
//
// For example:
//
//	input:  O'Reilly         → output:  'O''Reilly'
//	input:  2025-08-01       → output:  '2025-08-01'
//
// TODO move to pipe-helpers https://github.com/turbot/tailpipe/issues/517
func EscapeLiteral(literal string) string {
	escaped := strings.ReplaceAll(literal, `'`, `''`)
	return `'` + escaped + `'`
}
