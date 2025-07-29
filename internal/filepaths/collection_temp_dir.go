package filepaths

import (

	"github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/tailpipe/internal/config"
)

func EnsureCollectionTempDir() string {
	collectionDir := config.GlobalWorkspaceProfile.GetCollectionDir()
	pidTempDir := filepaths.EnsurePidTempDir(collectionDir)
	return pidTempDir
}
