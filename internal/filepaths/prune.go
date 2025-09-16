package filepaths

import (
	"os"
	"path/filepath"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
)

// PruneTree recursively deletes empty directories in the given folder.
func PruneTree(folder string) error {
	// if folder does not exist, return
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		return nil
	}
	isEmpty, err := pfilepaths.IsDirEmpty(folder)
	if err != nil {
		return err
	}

	if isEmpty {
		return os.Remove(folder)
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			err := PruneTree(filepath.Join(folder, entry.Name()))
			if err != nil {
				return err
			}
		}
	}

	// Check again if the folder is empty after pruning subdirectories
	isEmpty, err = pfilepaths.IsDirEmpty(folder)
	if err != nil {
		return err
	}

	if isEmpty {
		return os.Remove(folder)
	}

	return nil
}
