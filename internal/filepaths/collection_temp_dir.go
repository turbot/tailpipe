package filepaths

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
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

// CleanupPluginTmpDirs cleans up old tmp-* directories from the plugin directory
// These directories are created during plugin installation but may be left behind if the process crashes
func CleanupPluginTmpDirs() {
	pluginDir := filepath.Join(app_specific.InstallDir, "plugins")

	// ensure the plugin directory exists
	if _, err := os.Stat(pluginDir); os.IsNotExist(err) {
		slog.Debug("Plugin directory does not exist, skipping tmp cleanup", "dir", pluginDir)
		return
	}

	files, err := os.ReadDir(pluginDir)
	if err != nil {
		slog.Warn("failed to list files in plugin dir", "error", err, "dir", pluginDir)
		return
	}

	var cleanupCount int
	for _, file := range files {
		// look for directories that start with "tmp-"
		if file.IsDir() && len(file.Name()) > 4 && file.Name()[:4] == "tmp-" {
			tmpDir := filepath.Join(pluginDir, file.Name())
			slog.Debug("Removing plugin tmp directory", "dir", tmpDir)
			err := os.RemoveAll(tmpDir)
			if err != nil {
				slog.Warn("Failed to remove plugin tmp directory", "dir", tmpDir, "error", err)
			} else {
				cleanupCount++
				slog.Debug("Successfully removed plugin tmp directory", "dir", tmpDir)
			}
		}
	}

}
