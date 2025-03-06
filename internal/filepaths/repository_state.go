package filepaths

import (
	"path/filepath"

	"github.com/turbot/tailpipe/internal/config"
)

// GetLocalRepositoryStatePath is a function that returns the path to the local repository state file
// It can be overridden in tests to use a temporary directory
var GetLocalRepositoryStatePath = func() string {
	// return the path to the collection state file
	return filepath.Join(config.GlobalWorkspaceProfile.GetCollectionDir(), "local_repository_state.json")
}
