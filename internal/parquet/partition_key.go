package parquet

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"sort"
)

// partitionKey is used to uniquely identify a a combination of ducklake partition columns:
// tp_table, tp_partition, tp_index, year(tp_timestamp), month(tp_timestamp)
// It also stores the file and row stats for that partition key
type partitionKey struct {
	tpTable         string
	tpPartition     string
	tpIndex         string
	year            string // year(tp_timestamp) from partition value
	month           string // month(tp_timestamp) from partition value
	fileCount       int    // number of files for this partition key
	partitionConfig *config.Partition
}

// query the ducklake_data_file table to get all partition keys combinations which satisfy the provided patterns,
// along with the file and row stats for each partition key combination
func getPartitionKeysMatchingPattern(ctx context.Context, db *database.DuckDb, patterns []*PartitionPattern) ([]*partitionKey, error) {
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
		var pk = &partitionKey{}

		if err := rows.Scan(&pk.tpTable, &pk.tpPartition, &pk.tpIndex, &pk.year, &pk.month, &pk.fileCount); err != nil {
			return nil, fmt.Errorf("failed to scan partition key row: %w", err)
		}

		// retrieve the partition config for this key (which may not exist - that is ok
		partitionConfig, ok := config.GlobalConfig.Partitions[pk.partitionName()]
		if ok {
			pk.partitionConfig = partitionConfig
		}

		// check whether this partition key matches any of the provided patterns and whether there are any files
		if pk.fileCount > 0 && PartitionMatchesPatterns(pk.tpTable, pk.tpPartition, patterns) {
			partitionKeys = append(partitionKeys, pk)
		}
	}

	return partitionKeys, nil
}

// findOverlappingFileRanges finds sets of files that have overlapping time ranges and converts them to unorderedDataTimeRange
func (p *partitionKey) findOverlappingFileRanges(fileRanges []fileTimeRange) ([]unorderedDataTimeRange, error) {
	if len(fileRanges) <= 1 {
		return []unorderedDataTimeRange{}, nil
	}

	// Sort by start time - O(n log n)
	sort.Slice(fileRanges, func(i, j int) bool {
		return fileRanges[i].min.Before(fileRanges[j].min)
	})

	var unorderedRanges []unorderedDataTimeRange
	processedFiles := make(map[string]struct{})

	for i, currentFile := range fileRanges {
		if _, processed := processedFiles[currentFile.path]; processed {
			continue
		}

		// Find all files that overlap with this one
		overlappingFiles := p.findFilesOverlappingWith(currentFile, fileRanges[i+1:], processedFiles)

		// Only keep sets with multiple files (single files don't need compaction)
		if len(overlappingFiles) > 1 {
			// Convert overlapping files to unorderedDataTimeRange
			timeRange, err := newUnorderedDataTimeRange(overlappingFiles)
			if err != nil {
				return nil, fmt.Errorf("failed to create unordered time range: %w", err)
			}
			unorderedRanges = append(unorderedRanges, timeRange)
		}
	}

	return unorderedRanges, nil
}

// findFilesOverlappingWith finds all files that overlap with the given file
func (p *partitionKey) findFilesOverlappingWith(startFile fileTimeRange, remainingFiles []fileTimeRange, processedFiles map[string]struct{}) []fileTimeRange {
	overlappingFileRanges := []fileTimeRange{startFile}
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
		if p.fileOverlapsWithSet(candidateFile, overlappingFileRanges) {
			overlappingFileRanges = append(overlappingFileRanges, candidateFile)
			processedFiles[candidateFile.path] = struct{}{}

			// Update set's max end time
			if candidateFile.max.After(setMaxEnd) {
				setMaxEnd = candidateFile.max
			}
		}
	}

	return overlappingFileRanges
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

// return fully qualified partition name (table.partition)
func (p *partitionKey) partitionName() string {
	return fmt.Sprintf("%s.%s", p.tpTable, p.tpPartition)
}
