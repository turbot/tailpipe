package filepaths

import (
	"path/filepath"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

func TailpipeDbFilePath() string {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	return filepath.Join(dataDir, constants.TailpipeDbName)
}
