package parquet

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"golang.org/x/exp/maps"
	"time"
)

type CompactionStatus struct {
	InitialFiles  int
	FinalFiles    int
	RowsCompacted int64
	TotalRows     int64
	Progress      float64

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

func (s *CompactionStatus) Update(other CompactionStatus) {
	s.InitialFiles += other.InitialFiles
	s.FinalFiles += other.FinalFiles
	s.MigrateSource += other.MigrateSource
	s.MigrateDest += other.MigrateDest
	if s.PartitionIndexExpressions == nil {
		s.PartitionIndexExpressions = make(map[string]string)
	}
	s.Duration = other.Duration
	maps.Copy(s.PartitionIndexExpressions, other.PartitionIndexExpressions)
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
	if s.InitialFiles == 0 && s.FinalFiles == 0 {
		compactedString = "\nNo files to compact."
	} else {

		if s.InitialFiles > 0 {
			if len(uncompactedString) > 0 {
				uncompactedString = fmt.Sprintf(" (%s)", uncompactedString)
			}
			compactedString = fmt.Sprintf("Compacted %d files into %d files in %s.%s\n", s.InitialFiles, s.FinalFiles, s.Duration.String(), uncompactedString)
		} else {
			// Nothing compacted; show only uncompacted note if present
			compactedString = uncompactedString + "\n\n"
		}
	}

	return migratedString + compactedString
}

func (s *CompactionStatus) BriefString() string {
	if s.InitialFiles == 0 {
		return ""
	}

	uncompactedString := ""

	return fmt.Sprintf("Compacted %d files into %d files.%s\n", s.InitialFiles, s.FinalFiles, uncompactedString)
}
