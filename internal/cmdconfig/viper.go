package cmdconfig

import (
	"github.com/turbot/pipe-fittings/cmdconfig"
)

// BootstrapViper sets up viper with the essential path config (workspace-chdir and install-dir)
func BootstrapViper(opts ...cmdconfig.BootstrapOption) {
	config := cmdconfig.NewBootstrapConfig()
	for _, opt := range opts {
		opt(config)
	}

	// set defaults  for keys which do not have a corresponding command flag
	cmdconfig.SetBaseDefaults(config.ConfigDefaults)

	// set defaults for install dir and mod location from env vars
	// this needs to be done since the workspace profile definitions exist in the
	// default install dir
	cmdconfig.SetDefaultsFromEnv(config.DirectoryEnvMappings)
}
