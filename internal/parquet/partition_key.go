package parquet

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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

// buildUnorderedTimeRanges finds groups of files with overlapping timestamp ranges
func (p *partitionKey) buildUnorderedTimeRanges(fileRanges []fileTimeRange) ([]unorderedDataTimeRange, error) {
	if len(fileRanges) <= 1 {
		return []unorderedDataTimeRange{}, nil
	}

	// Find sets of overlapping files
	overlappingFileGroups := p.findOverlappingFileGroups(fileRanges)

	// Convert to unorderedDataTimeRange structs with metadata (rowcount, start/end time for time range)
	var unorderedRanges []unorderedDataTimeRange
	for _, fileGroup := range overlappingFileGroups {
		timeRanges, err := newUnorderedDataTimeRange(fileGroup)
		if err != nil {
			return nil, fmt.Errorf("failed to create overlapping file set: %w", err)
		}
		unorderedRanges = append(unorderedRanges, timeRanges)
	}
	return unorderedRanges, nil
}

// findOverlappingFileGroups finds sets of files that have overlapping time ranges
func (p *partitionKey) findOverlappingFileGroups(fileRanges []fileTimeRange) [][]fileTimeRange {
	// Sort by start time - O(n log n)
	sort.Slice(fileRanges, func(i, j int) bool {
		return fileRanges[i].min.Before(fileRanges[j].min)
	})

	var unorderedRanges [][]fileTimeRange
	processedFiles := make(map[string]struct{})

	for i, currentFile := range fileRanges {
		if _, processed := processedFiles[currentFile.path]; processed {
			continue
		}

		// Find all files that overlap with this one
		overlappingFiles := p.findFilesOverlappingWith(currentFile, fileRanges[i+1:], processedFiles)

		// Only keep sets with multiple files (single files don't need compaction)
		if len(overlappingFiles) > 1 {
			unorderedRanges = append(unorderedRanges, overlappingFiles)
		}
	}

	return unorderedRanges
}

// findFilesOverlappingWith finds all files that overlap with the given file
func (p *partitionKey) findFilesOverlappingWith(startFile fileTimeRange, remainingFiles []fileTimeRange, processedFiles map[string]struct{}) []fileTimeRange {
	unorderedRanges := []fileTimeRange{startFile}
	processedFiles[startFile.path] = struct{}{}
	setMaxEnd := startFile.max

	for _, candidateFile := range remainingFiles {
		if _, processed := processedFiles[candidateFile.path]; processed {
			continue
		}

		// Early termination: if candidate starts after set ends, no more overlaps
		if candidateFile.min.After(setMaxEnd) {
			break
		}

		// Check if this file overlaps with any file in our set
		if p.fileOverlapsWithSet(candidateFile, unorderedRanges) {
			unorderedRanges = append(unorderedRanges, candidateFile)
			processedFiles[candidateFile.path] = struct{}{}

			// Update set's max end time
			if candidateFile.max.After(setMaxEnd) {
				setMaxEnd = candidateFile.max
			}
		}
	}

	return unorderedRanges
}

// fileOverlapsWithSet checks if a file overlaps with any file in the set
func (p *partitionKey) fileOverlapsWithSet(candidateFile fileTimeRange, fileSet []fileTimeRange) bool {
	for _, setFile := range fileSet {
		if rangesOverlap(setFile, candidateFile) {
			return true
		}
	}
	return false
}
