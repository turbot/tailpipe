package parquet

import (
	"fmt"
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

	var uncompactedString, compactedString string
	if s.InitialFiles == 0 && s.FinalFiles == 0 || s.RowsCompacted == 0 {
		compactedString = "\nNo files required compaction."
		// Did we compact any files
		if s.InitialFiles > 0 && s.FinalFiles != s.InitialFiles {
			if len(uncompactedString) > 0 {
				uncompactedString = fmt.Sprintf(" (%s)", uncompactedString)
			}
			// if the file count is the same, we must have just ordered
			if s.InitialFiles == s.FinalFiles {
				compactedString = fmt.Sprintf("Ordered %d rows in %dfiles in %s.%s\n", s.TotalRows, s.InitialFiles, s.Duration.String(), uncompactedString)
			} else {
				compactedString = fmt.Sprintf("Compacted and ordered %d rows in %d files into %d files in %s.%s\n", s.TotalRows, s.InitialFiles, s.FinalFiles, s.Duration.String(), uncompactedString)
			}

		} else {
			// Nothing compacted; show only uncompacted note if present
			compactedString = uncompactedString + "\n\n"
		}
	}

	return migratedString + compactedString
}
