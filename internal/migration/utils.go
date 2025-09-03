package migration

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/turbot/tailpipe/internal/config"
)

// findMatchingTableDirs lists subdirectories of baseDir whose names start with
// "tp_table=" and whose table names exist in the provided tables slice.
// Also returns unmatched tp_table directories for which there is no view in the DB.
func findMatchingTableDirs(baseDir string, tables []string) ([]string, []string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, nil, err
	}
	tableSet := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		tableSet[t] = struct{}{}
	}
	var matches []string
	var unmatched []string
	const prefix = "tp_table="
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		tableName := strings.TrimPrefix(name, prefix)
		if _, ok := tableSet[tableName]; ok {
			matches = append(matches, filepath.Join(baseDir, name))
		} else {
			unmatched = append(unmatched, filepath.Join(baseDir, name))
		}
	}
	return matches, unmatched, nil
}

// moveLegacyDbFile moves tailpipe.db from the migrating/default folder to migrated/default if it exists.
func moveLegacyDbFile(migratingDefaultDir string) error {
	src := filepath.Join(migratingDefaultDir, "tailpipe.db")
	if _, err := os.Stat(src); err != nil {
		// if not present, nothing to do
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	migratedDefaultDir := config.GlobalWorkspaceProfile.GetMigratedDir()
	if err := os.MkdirAll(migratedDefaultDir, 0755); err != nil {
		return err
	}
	dest := filepath.Join(migratedDefaultDir, "tailpipe.db")
	return os.Rename(src, dest)
}

// hasTailpipeDb checks if a tailpipe.db file exists in the provided directory.
func hasTailpipeDb(dir string) bool {
	if dir == "" {
		return false
	}
	p := filepath.Join(dir, "tailpipe.db")
	_, err := os.Stat(p)
	return err == nil
}
