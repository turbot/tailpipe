package parquet

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/turbot/pipe-fittings/v2/utils"
	"time"
)

type CompactionStatus struct {
	InitialFiles    int
	FinalFiles      int
	RowsCompacted   int64
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
