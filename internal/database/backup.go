package database

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/turbot/tailpipe/internal/config"
)

// BackupDucklakeMetadata creates a timestamped backup of the DuckLake metadata database.
// It creates a backup file with format: tailpipe_ducklake.db.backup.YYYYMMDDHHMMSS
// and removes any existing backup files to maintain only the most recent backup.
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

	// Create backup filename
	dbDir := filepath.Dir(dbPath)
	backupFilename := fmt.Sprintf("tailpipe_ducklake.db.backup.%s", timestamp)
	backupPath := filepath.Join(dbDir, backupFilename)

	slog.Info("Creating backup of DuckLake metadata database", "source", dbPath, "backup", backupPath)

	// Clean up any existing backup files before creating new one
	if err := cleanupOldBackups(dbDir); err != nil {
		slog.Warn("Failed to clean up old backup files", "error", err)
		// Continue with backup creation even if cleanup fails
	}

	// Create the backup by copying the database file
	if err := copyFile(dbPath, backupPath); err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	slog.Info("Successfully created backup of DuckLake metadata database", "backup", backupPath)
	return nil
}

// cleanupOldBackups removes all existing backup files in the specified directory.
// Backup files are identified by the pattern: tailpipe_ducklake.db.backup.*
func cleanupOldBackups(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	backupPrefix := "tailpipe_ducklake.db.backup."
	var deletedCount int

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if strings.HasPrefix(filename, backupPrefix) {
			backupPath := filepath.Join(dir, filename)
			if err := os.Remove(backupPath); err != nil {
				slog.Warn("Failed to remove old backup file", "file", backupPath, "error", err)
				// Continue removing other files even if one fails
			} else {
				slog.Debug("Removed old backup file", "file", backupPath)
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		slog.Debug("Cleaned up old backup files", "count", deletedCount)
	}

	return nil
}

// copyFile copies a file from src to dst, preserving file permissions.
// It creates the destination file and copies the content using io.Copy.
func copyFile(src, dst string) error {
	// Open source file
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer srcFile.Close()

	// Get source file info for permissions
	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get source file info: %w", err)
	}

	// Create destination file
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer dstFile.Close()

	// Copy content
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	// Ensure data is written to disk
	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}

	return nil
}
