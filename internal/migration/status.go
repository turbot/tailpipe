package migration

import (
	"fmt"
	"strings"
	"time"

	perr "github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/tailpipe/internal/config"
)

type MigrationStatus struct {
	Status          string        `json:"status"`
	Total           int           `json:"total"`
	Migrated        int           `json:"migrated"`
	Failed          int           `json:"failed"`
	Remaining       int           `json:"remaining"`
	ProgressPercent float64       `json:"progress_percent"`
	FailedTables    []string      `json:"failed_tables,omitempty"`
	StartTime       time.Time     `json:"start_time"`
	Duration        time.Duration `json:"duration"`
}

func NewMigrationStatus(total int) *MigrationStatus {
	return &MigrationStatus{Total: total, Remaining: total, StartTime: time.Now()}
}

func (s *MigrationStatus) OnTableMigrated() {
	s.Migrated++
	s.update()
}

func (s *MigrationStatus) OnTableFailed(tableName string) {
	s.Failed++
	s.FailedTables = append(s.FailedTables, tableName)
	s.update()
}

func (s *MigrationStatus) update() {
	s.Remaining = s.Total - s.Migrated - s.Failed
	if s.Total > 0 {
		s.ProgressPercent = float64(s.Migrated) * 100.0 / float64(s.Total)
	}
}

func (s *MigrationStatus) Finish(outcome string) {
	s.Status = outcome
	s.Duration = time.Since(s.StartTime)
}

// StatusMessage prints a user-facing status message (with stats) based on current status
func (s *MigrationStatus) StatusMessage() {
	migratedDir := config.GlobalWorkspaceProfile.GetMigratedDir()
	failedDir := config.GlobalWorkspaceProfile.GetMigrationFailedDir()
	migratingDir := config.GlobalWorkspaceProfile.GetMigratingDir()

	switch s.Status {
	case "SUCCESS":
		perr.ShowWarning(fmt.Sprintf(
			"DuckLake migration complete.\n"+
				"Tables migrated: %d of %d \n"+
				"Failed: %d\n"+
				"Remaining: %d\n"+
				"Legacy data has been backed up to '%s'.\n",
			s.Migrated, s.Total, s.Failed, s.Remaining, migratedDir,
		))
	case "CANCELLED":
		perr.ShowWarning(fmt.Sprintf(
			"DuckLake migration cancelled.\n"+
				"Tables migrated: %d of %d \n"+
				"Failed: %d\n"+
				"Remaining: %d\n"+
				"Migration can be resumed on the next run of tailpipe.\n"+
				"Legacy DB is preserved at '%s/tailpipe.db'.\n",
			s.Migrated, s.Total, s.Failed, s.Remaining, migratingDir,
		))
	case "INCOMPLETE":
		failedList := "(none)"
		if len(s.FailedTables) > 0 {
			failedList = strings.Join(s.FailedTables, ", ")
		}
		perr.ShowWarning(fmt.Sprintf(
			"DuckLake migration completed with issues.\n"+
				"Tables migrated: %d of %d \n"+
				"Failed (%d): %s\n"+
				"Remaining: %d\n"+
				"Failed table data and legacy DB have been moved to '%s'.\n"+
				"Legacy data has been backed up to '%s'\n",
			s.Migrated, s.Total, s.Failed, failedList, s.Remaining, failedDir, migratedDir,
		))
	}
}
