package filepaths

import (
	"fmt"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe/internal/config"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
)

func GetCollectionTempDir() string {
	collectionDir := config.GlobalWorkspaceProfile.GetCollectionDir()
	// cleanup the collection temp dir from previous runs
	cleanupCollectionTempDirs(collectionDir)

	return filepath.Join(collectionDir, fmt.Sprintf("%d", os.Getpid()))
}
func cleanupCollectionTempDirs(collectionTempDir string) {
	// list all folders alongside our collection temp dir
	parent := filepath.Dir(collectionTempDir)
	files, err := os.ReadDir(parent)
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
					slog.Info(fmt.Sprintf("cleanupCollectionTempDirs skipping directory '%s' as process  with PID %d exists", file.Name(), pid))
					continue
				}
			}
			slog.Debug("removing directory", "dir", file.Name())
			_ = os.RemoveAll(filepath.Join(parent, file.Name()))
		}
	}
}
