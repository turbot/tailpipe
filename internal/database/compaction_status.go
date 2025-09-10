package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/backend"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/utils"
)

type CompactionStatus struct {
	Message         string
	InitialFiles    int
	FinalFiles      int
	RowsCompacted   int64
	RowsToCompact   int64
	TotalRows       int64
	ProgressPercent float64

	MigrateSource             int               // number of source files migrated
	MigrateDest               int               // number of destination files after migration
	PartitionIndexExpressions map[string]string // the index expression used for migration for each partition
	Duration                  time.Duration     // duration of the compaction process
}

func NewCompactionStatus() *CompactionStatus {
	return &CompactionStatus{
		PartitionIndexExpressions: make(map[string]string),
	}
}

func (s *CompactionStatus) VerboseString() string {
	var migratedString string
	// Show migration status for each partition if any
	if s.MigrateSource > 0 {
		migratedString = fmt.Sprintf(`Migrated tp_index for %d %s`,
			len(s.PartitionIndexExpressions),
			utils.Pluralize("partition", len(s.PartitionIndexExpressions)),
		)
		if s.MigrateSource != s.MigrateDest {
			migratedString += fmt.Sprintf(" (%d %s migrated to %d %s)",
				s.MigrateSource,
				utils.Pluralize("file", s.MigrateSource),
				s.MigrateDest,
				utils.Pluralize("file", s.MigrateDest))
		}
		migratedString += ".\n"
	}

	var compactedString string
	if s.RowsCompacted == 0 {
		compactedString = "\nNo files required compaction."
	} else {
		// if the file count is the same, we must have just ordered
		if s.InitialFiles == s.FinalFiles {
			compactedString = fmt.Sprintf("Ordered %s rows in %s files (%s).\n", s.TotalRowsString(), s.InitialFilesString(), s.DurationString())
		} else {
			compactedString = fmt.Sprintf("Compacted and ordered %s rows in %s files into %s files in (%s).\n", s.TotalRowsString(), s.InitialFilesString(), s.FinalFilesString(), s.DurationString())
		}
	}

	return migratedString + compactedString
}

func (s *CompactionStatus) String() string {
	var migratedString string
	var compactedString string
	if s.RowsCompacted == 0 {
		compactedString = "No files required compaction."
	} else {
		// if the file count is the same, we must have just ordered
		if s.InitialFiles == s.FinalFiles {
			compactedString = fmt.Sprintf("Ordered %s rows in %s files in %s.\n", s.TotalRowsString(), s.InitialFilesString(), s.Duration.String())
		} else {
			compactedString = fmt.Sprintf("Compacted and ordered %s rows in %s files into %s files in %s.\n", s.TotalRowsString(), s.InitialFilesString(), s.FinalFilesString(), s.Duration.String())
		}
	}

	return migratedString + compactedString
}

func (s *CompactionStatus) TotalRowsString() any {
	return humanize.Comma(s.TotalRows)
}
func (s *CompactionStatus) InitialFilesString() any {
	return humanize.Comma(int64(s.InitialFiles))
}
func (s *CompactionStatus) FinalFilesString() any {
	return humanize.Comma(int64(s.FinalFiles))
}
func (s *CompactionStatus) DurationString() string {
	return utils.HumanizeDuration(s.Duration)
}
func (s *CompactionStatus) RowsCompactedString() any {
	return humanize.Comma(s.RowsCompacted)
}
func (s *CompactionStatus) ProgressPercentString() string {
	return fmt.Sprintf("%.1f%%", s.ProgressPercent)
}

func (s *CompactionStatus) UpdateProgress() {
	// calc percentage from RowsToCompact but print TotalRows in status message
	s.ProgressPercent = (float64(s.RowsCompacted) / float64(s.RowsToCompact)) * 100
	s.Message = fmt.Sprintf(" (%0.1f%% of %s rows)", s.ProgressPercent, types.ToHumanisedString(s.TotalRows))

}

func (s *CompactionStatus) getInitialCounts(ctx context.Context, db *DuckDb, partitionKeys []*partitionKey) error {
	partitionNameMap := make(map[string]map[string]struct{})
	for _, pk := range partitionKeys {
		s.InitialFiles += pk.fileCount
		if partitionNameMap[pk.tpTable] == nil {
			partitionNameMap[pk.tpTable] = make(map[string]struct{})
		}
		partitionNameMap[pk.tpTable][pk.tpPartition] = struct{}{}
	}

	// get row count for each table
	totalRows := int64(0)
	for tpTable, tpPartitions := range partitionNameMap {

		// Sanitize partition values for SQL injection protection
		sanitizedPartitions := make([]string, 0, len(tpPartitions))
		for partition := range tpPartitions {
			sp, err := backend.SanitizeDuckDBIdentifier(partition)
			if err != nil {
				return fmt.Errorf("failed to sanitize partition %s: %w", partition, err)
			}
			// Quote the sanitized partition name for the IN clause
			sanitizedPartitions = append(sanitizedPartitions, fmt.Sprintf("'%s'", sp))
		}

		tableName, err := backend.SanitizeDuckDBIdentifier(tpTable)
		if err != nil {
			return fmt.Errorf("failed to sanitize table name %s: %w", tpTable, err)
		}

		query := fmt.Sprintf("select count(*) from %s where tp_partition in (%s)",
			tableName,
			strings.Join(sanitizedPartitions, ", "))

		var tableRowCount int64
		err = db.QueryRowContext(ctx, query).Scan(&tableRowCount)
		if err != nil {
			return fmt.Errorf("failed to get row count for table %s: %w", tpTable, err)
		}

		totalRows += tableRowCount
	}

	s.TotalRows = totalRows
	return nil
}

func (s *CompactionStatus) getFinalFileCounts(ctx context.Context, db *DuckDb, partitionKeys []*partitionKey) error {
	// Get unique table names from partition keys
	tableNames := make(map[string]struct{})
	for _, pk := range partitionKeys {
		tableNames[pk.tpTable] = struct{}{}
	}

	// Count files for each table from metadata
	totalFileCount := 0
	for tableName := range tableNames {
		// Sanitize table name
		sanitizedTableName, err := backend.SanitizeDuckDBIdentifier(tableName)
		if err != nil {
			return fmt.Errorf("failed to sanitize table name %s: %w", tableName, err)
		}

		// Query to count files for this table from DuckLake metadata
		query := fmt.Sprintf(`select count(*) from %s.ducklake_data_file df
			join %s.ducklake_table t on df.table_id = t.table_id
			where t.table_name = '%s' and df.end_snapshot is null`,
			constants.DuckLakeMetadataCatalog,
			constants.DuckLakeMetadataCatalog,
			sanitizedTableName)

		var tableFileCount int
		err = db.QueryRowContext(ctx, query).Scan(&tableFileCount)
		if err != nil {
			return fmt.Errorf("failed to get file count for table %s: %w", tableName, err)
		}

		totalFileCount += tableFileCount
	}

	s.FinalFiles = totalFileCount
	return nil
}
