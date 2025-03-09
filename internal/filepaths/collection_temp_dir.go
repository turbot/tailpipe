package filepaths

import (
	"fmt"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
)

func EnsureCollectionTempDir() string {
	collectionDir := config.GlobalWorkspaceProfile.GetCollectionDir()

	// add a PID directory to the collection directory
	collectionTempDir := filepath.Join(collectionDir, fmt.Sprintf("%d", os.Getpid()))

	// create the directory if it doesn't exist
	if _, err := os.Stat(collectionTempDir); os.IsNotExist(err) {
		err := os.MkdirAll(collectionTempDir, 0755)
		if err != nil {
			slog.Error("failed to create collection temp dir", "error", err)
		}
	}
	return collectionTempDir
}

func CleanupCollectionTempDirs() {
	// get the collection directory for this workspace
	collectionDir := config.GlobalWorkspaceProfile.GetCollectionDir()

	files, err := os.ReadDir(collectionDir)
	if err != nil {
		slog.Warn("failed to list files in collection dir", "error", err)
		return
	}
	for _, file := range files {
		// if the file is a directory and is not our collection temp dir, remove it
		if file.IsDir() {
			// the folder name is the PID - check whether that pid exists
			// if it doesn't, remove the folder
			// Attempt to find the process
			// try to parse the directory name as a pid
			pid, err := strconv.ParseInt(file.Name(), 10, 32)
			if err == nil {
				if utils.PidExists(int(pid)) {
					slog.Info(fmt.Sprintf("Cleaning existing collection temp dirs - skipping directory '%s' as process with PID %d exists", file.Name(), pid))
					continue
				}
			}
			slog.Debug("Removing directory", "dir", file.Name())
			_ = os.RemoveAll(filepath.Join(collectionDir, file.Name()))
		}
	}
}
