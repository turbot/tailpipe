package migration

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/config"
)

type MigrationStatus struct {
	Status          string  `json:"status"`
	Total           int     `json:"total"`
	Migrated        int     `json:"migrated"`
	Failed          int     `json:"failed"`
	Remaining       int     `json:"remaining"`
	ProgressPercent float64 `json:"progress_percent"`

	TotalFiles     int `json:"total_files"`
	MigratedFiles  int `json:"migrated_files"`
	FailedFiles    int `json:"failed_files"`
	RemainingFiles int `json:"remaining_files"`

	FailedTables []string      `json:"failed_tables,omitempty"`
	StartTime    time.Time     `json:"start_time"`
	Duration     time.Duration `json:"duration"`

	Errors []string `json:"errors,omitempty"`
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

func (s *MigrationStatus) OnFilesMigrated(n int) {
	if n <= 0 {
		return
	}
	s.MigratedFiles += n
	s.updateFiles()
}

func (s *MigrationStatus) OnFilesFailed(n int) {
	if n <= 0 {
		return
	}
	s.FailedFiles += n
	s.updateFiles()
}

func (s *MigrationStatus) AddError(err error) {
	if err == nil {
		return
	}
	s.Errors = append(s.Errors, err.Error())
}

func (s *MigrationStatus) update() {
	s.Remaining = s.Total - s.Migrated - s.Failed
	if s.Total > 0 {
		s.ProgressPercent = float64(s.Migrated) * 100.0 / float64(s.Total)
	}
}

func (s *MigrationStatus) updateFiles() {
	s.RemainingFiles = s.TotalFiles - s.MigratedFiles - s.FailedFiles
}

func (s *MigrationStatus) Finish(outcome string) {
	s.Status = outcome
	s.Duration = time.Since(s.StartTime)
}

// StatusMessage returns a user-facing status message (with stats) based on current migration status
func (s *MigrationStatus) StatusMessage() string {
	migratedDir := config.GlobalWorkspaceProfile.GetMigratedDir()
	failedDir := config.GlobalWorkspaceProfile.GetMigrationFailedDir()
	migratingDir := config.GlobalWorkspaceProfile.GetMigratingDir()

	switch s.Status {
	case "SUCCESS":
		return fmt.Sprintf(
			"DuckLake migration complete.\n"+
				"- Tables: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Parquet files: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Backup of migrated legacy data: '%s'\n",
			s.Migrated, s.Total, s.Failed, s.Remaining,
			s.MigratedFiles, s.TotalFiles, s.FailedFiles, s.RemainingFiles,
			migratedDir,
		)
	case "CANCELLED":
		return fmt.Sprintf(
			"DuckLake migration cancelled.\n"+
				"- Tables: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Parquet files: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Legacy DB preserved: '%s/tailpipe.db'\n\n"+
				"Re-run Tailpipe to resume migrating your data.\n",
			s.Migrated, s.Total, s.Failed, s.Remaining,
			s.MigratedFiles, s.TotalFiles, s.FailedFiles, s.RemainingFiles,
			migratingDir,
		)
	case "INCOMPLETE":
		failedList := "(none)"
		if len(s.FailedTables) > 0 {
			failedList = strings.Join(s.FailedTables, ", ")
		}
		base := fmt.Sprintf(
			"DuckLake migration completed with issues.\n"+
				"- Tables: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Parquet files: %d/%d migrated (failed: %d, remaining: %d)\n"+
				"- Failed tables (%d): %s\n"+
				"- Failed data and legacy DB: '%s'\n"+
				"- Backup of migrated legacy data: '%s'\n",
			s.Migrated, s.Total, s.Failed, s.Remaining,
			s.MigratedFiles, s.TotalFiles, s.FailedFiles, s.RemainingFiles,
			len(s.FailedTables), failedList,
			failedDir,
			migratedDir,
		)
		if len(s.Errors) > 0 {
			base += fmt.Sprintf("\nErrors: %d error(s) occurred during migration\n", len(s.Errors))
			base += "Details:\n"
			for _, e := range s.Errors {
				base += "- " + e + "\n"
			}
		}
		return base
	default:
		return "DuckLake migration status unknown"
	}
}

// WriteStatusToFile writes the status message to a migration stats file under the migration directory.
// The file is overwritten on each run (resume will update it).
func (s *MigrationStatus) WriteStatusToFile() error {
	// Place the file under the migration root (e.g., ~/.tailpipe/migration/migration.log)
	migrationRootDir := config.GlobalWorkspaceProfile.GetMigrationDir()
	statsFile := filepath.Join(migrationRootDir, "migration.log")
	msg := s.StatusMessage()
	if err := os.MkdirAll(migrationRootDir, 0755); err != nil {
		return err
	}
	return os.WriteFile(statsFile, []byte(msg), 0600)
}
