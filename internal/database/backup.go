package database

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
)

// BackupDucklakeMetadata creates a timestamped backup of the DuckLake metadata database.
// It creates backup files with format: metadata.sqlite.backup.YYYYMMDDHHMMSS
// and also backs up the WAL file if it exists:
// - metadata.sqlite-wal.backup.YYYYMMDDHHMMSS
// It removes any existing backup files to maintain only the most recent backup.
//
// The backup is created in the same directory as the original database file.
// If the database file doesn't exist, no backup is created and no error is returned.
//
// Returns an error if the backup operation fails.
func BackupDucklakeMetadata() error {
	// Get the path to the DuckLake metadata database
	dbPath := config.GlobalWorkspaceProfile.GetDucklakeDbPath()

	// Check if the database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		slog.Debug("DuckLake metadata database does not exist, skipping backup", "path", dbPath)
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	}

	// Generate timestamp for backup filename
	timestamp := time.Now().Format("20060102150405") // YYYYMMDDHHMMSS format

	// Create backup filenames
	dbDir := filepath.Dir(dbPath)
	mainBackupFilename := fmt.Sprintf("metadata.sqlite.backup.%s", timestamp)
	mainBackupPath := filepath.Join(dbDir, mainBackupFilename)

	// Also prepare paths for WAL file
	walPath := dbPath + "-wal"
	walBackupFilename := fmt.Sprintf("metadata.sqlite-wal.backup.%s", timestamp)
	walBackupPath := filepath.Join(dbDir, walBackupFilename)

	slog.Info("Creating backup of DuckLake metadata database", "source", dbPath, "backup", mainBackupPath)

	// Create the main database backup first
	if err := utils.CopyFile(dbPath, mainBackupPath); err != nil {
		return fmt.Errorf("failed to create main database backup: %w", err)
	}

	// Backup WAL file if it exists
	if _, err := os.Stat(walPath); err == nil {
		if err := utils.CopyFile(walPath, walBackupPath); err != nil {
			slog.Warn("Failed to backup WAL file", "source", walPath, "error", err)
			// Continue - WAL backup failure is not critical
		} else {
			slog.Debug("Successfully backed up WAL file", "backup", walBackupPath)
		}
	}

	slog.Info("Successfully created backup of DuckLake metadata database", "backup", mainBackupPath)

	// Clean up old backup files after successfully creating the new one
	if err := cleanupOldBackups(dbDir, timestamp); err != nil {
		slog.Warn("Failed to clean up old backup files", "error", err)
		// Don't return error - the backup was successful, cleanup is just housekeeping
	}
	return nil
}

// isBackupFile checks if a filename matches any of the backup patterns
func isBackupFile(filename string) bool {
	backupPrefixes := []string{
		"metadata.sqlite.backup.",
		"metadata.sqlite-wal.backup.",
	}

	for _, prefix := range backupPrefixes {
		if strings.HasPrefix(filename, prefix) {
			return true
		}
	}
	return false
}

// shouldRemoveBackup determines if a backup file should be removed
func shouldRemoveBackup(filename, excludeTimestamp string) bool {
	if !isBackupFile(filename) {
		return false
	}
	// Don't remove files with the current timestamp
	return !strings.HasSuffix(filename, "."+excludeTimestamp)
}

// cleanupOldBackups removes all existing backup files in the specified directory,
// except for the newly created backup files with the given timestamp.
// Backup files are identified by the patterns:
// - metadata.sqlite.backup.*
// - metadata.sqlite-wal.backup.*
func cleanupOldBackups(dir, excludeTimestamp string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	var deletedCount int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !shouldRemoveBackup(filename, excludeTimestamp) {
			continue
		}

		backupPath := filepath.Join(dir, filename)
		if err := os.Remove(backupPath); err != nil {
			slog.Warn("Failed to remove old backup file", "file", backupPath, "error", err)
			// Continue removing other files even if one fails
		} else {
			slog.Debug("Removed old backup file", "file", backupPath)
			deletedCount++
		}
	}

	if deletedCount > 0 {
		slog.Debug("Cleaned up old backup files", "count", deletedCount)
	}

	return nil
}
