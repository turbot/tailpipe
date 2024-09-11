package filepaths

import (
	"fmt"
	"github.com/turbot/tailpipe/internal/config"
	"os"
	"path/filepath"
)

// EnsureSourcePath ensures the source path exists - this is the folder where the plugin writes JSONL files
func EnsureSourcePath() (string, error) {
	sourceFilePath := filepath.Join(config.GlobalWorkspaceProfile.GetInternalDir(), "source")
	// ensure it exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		err = os.MkdirAll(sourceFilePath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create source directory %s: %w", sourceFilePath, err)
		}
	}

	return sourceFilePath, nil
}
