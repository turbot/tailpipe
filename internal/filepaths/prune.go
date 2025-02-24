package filepaths

import (
	"io"
	"os"
	"path/filepath"
)

// PruneTree recursively deletes empty directories in the given folder.
func PruneTree(folder string) error {
	// if folder does not exist, return
	if _, err := os.Stat(folder); os.IsNotExist(err) {
		return nil
	}
	isEmpty, err := isDirEmpty(folder)
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
	isEmpty, err = isDirEmpty(folder)
	if err != nil {
		return err
	}

	if isEmpty {
		return os.Remove(folder)
	}

	return nil
}

// isDirEmpty checks if a directory is empty.
func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}
