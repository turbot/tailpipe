package database

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// getTimeRangesToReorder analyzes file fragmentation and creates disorder metrics for a partition key.
// It queries DuckLake metadata to get all files for the partition, their timestamp ranges, and row counts.
// Then it identifies groups of files with overlapping time ranges that need compaction.
// Returns metrics including total file count and overlapping file sets with their metadata.
func getTimeRangesToReorder(ctx context.Context, db *DuckDb, pk *partitionKey, reindex bool) (*reorderMetadata, error) {
	// NOTE: if we are reindexing, we must rewrite the entire partition key
	//  - return a single range for the entire partition key
	if reindex {
		rm, err := newReorderMetadata(ctx, db, pk)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve stats for partition key: %w", err)
		}

		// make a single time range
		rm.unorderedRanges = []unorderedDataTimeRange{
			{
				StartTime: rm.minTimestamp,
				EndTime:   rm.maxTimestamp,
				RowCount:  rm.rowCount,
			},
		}

		return rm, nil
	}

	// first query the metadata to get a list of files, their timestamp ranges and row counts for this partition key
	fileRanges, err := getFileRangesForPartitionKey(ctx, db, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to get file ranges for partition key: %w", err)
	}

	// Now identify which of these ranges overlap and for each overlapping set, build a superset time range
	unorderedRanges, err := pk.findOverlappingFileRanges(fileRanges)
	if err != nil {
		return nil, fmt.Errorf("failed to build unordered time ranges: %w", err)
	}

	// if there are no unordered ranges, return nil
	if len(unorderedRanges) == 0 {
		return nil, nil
	}

	// get stats for the partition key
	rm, err := newReorderMetadata(ctx, db, pk)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve stats for partition key: %w", err)
	}
	rm.unorderedRanges = unorderedRanges
	return rm, nil

}

// query the metadata to get a list of files, their timestamp ranges and row counts for this partition key
func getFileRangesForPartitionKey(ctx context.Context, db *DuckDb, pk *partitionKey) ([]fileTimeRange, error) {
	query := `select 
		df.path,
		cast(fcs.min_value as timestamp) as min_timestamp,
		cast(fcs.max_value as timestamp) as max_timestamp,
		df.record_count
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
	  and df.table_id = fcs.table_id
	join __ducklake_metadata_tailpipe_ducklake.ducklake_column c
	  on fcs.column_id = c.column_id
	  and fcs.table_id = c.table_id
	where t.table_name = ?
	  and fpv1.partition_value = ?
	  and fpv2.partition_value = ?
	  and fpv3.partition_value = ?
	  and fpv4.partition_value = ?
	  and c.column_name = 'tp_timestamp'
	  and df.end_snapshot is null
	  and c.end_snapshot is null
	order by df.data_file_id`

	rows, err := db.QueryContext(ctx, query, pk.tpTable, pk.tpPartition, pk.tpIndex, pk.year, pk.month)
	if err != nil {
		return nil, fmt.Errorf("failed to get file timestamp ranges: %w", err)
	}
	defer rows.Close()

	var fileRanges []fileTimeRange
	for rows.Next() {
		var path string
		var minTime, maxTime time.Time
		var rowCount int64
		if err := rows.Scan(&path, &minTime, &maxTime, &rowCount); err != nil {
			return nil, fmt.Errorf("failed to scan file range: %w", err)
		}
		fileRanges = append(fileRanges, fileTimeRange{path: path, min: minTime, max: maxTime, rowCount: rowCount})
	}

	totalFiles := len(fileRanges)
	if totalFiles <= 1 {
		return nil, nil
	}

	// build string for the ranges
	var rangesStr strings.Builder
	for i, file := range fileRanges {
		rangesStr.WriteString(fmt.Sprintf("start: %s, end: %s", file.min.String(), file.max.String()))
		if i < len(fileRanges)-1 {
			rangesStr.WriteString(", ")
		}
	}
	return fileRanges, nil
}

type fileTimeRange struct {
	path     string
	min      time.Time
	max      time.Time
	rowCount int64
}

// unorderedDataTimeRange represents a time range containing unordered data that needs reordering
type unorderedDataTimeRange struct {
	StartTime time.Time // start of the time range containing unordered data
	EndTime   time.Time // end of the time range containing unordered data
	RowCount  int64     // total row count in this time range
}

// newUnorderedDataTimeRange creates a single unorderedDataTimeRange from overlapping files
func newUnorderedDataTimeRange(overlappingFiles []fileTimeRange) (unorderedDataTimeRange, error) {
	var rowCount int64
	var startTime, endTime time.Time

	// Single loop to sum row counts and calculate time range
	for i, file := range overlappingFiles {
		rowCount += file.rowCount

		// Calculate time range
		if i == 0 {
			startTime = file.min
			endTime = file.max
		} else {
			if file.min.Before(startTime) {
				startTime = file.min
			}
			if file.max.After(endTime) {
				endTime = file.max
			}
		}
	}

	return unorderedDataTimeRange{
		StartTime: startTime,
		EndTime:   endTime,
		RowCount:  rowCount,
	}, nil
}

// rangesOverlap checks if two timestamp ranges overlap (excluding contiguous ranges)
func rangesOverlap(r1, r2 fileTimeRange) bool {
	// Two ranges overlap if one starts before the other ends AND they're not just touching
	// Contiguous ranges (where one ends exactly when the other starts) are NOT considered overlapping
	return r1.min.Before(r2.max) && r2.min.Before(r1.max)
}
