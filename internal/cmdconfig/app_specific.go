package cmdconfig

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/files"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/filepaths"
	"github.com/turbot/tailpipe/internal/constants"
)

// SetAppSpecificConstants sets app specific constants defined in pipe-fittings
func SetAppSpecificConstants() {
	app_specific.AppName = "tailpipe"

	versionString := viper.GetString("main.version")
	app_specific.AppVersion = semver.MustParse(versionString)

	app_specific.PluginHub = constants.TailpipeHubOCIBase
	// set all app specific env var keys
	app_specific.SetAppSpecificEnvVarKeys("TAILPIPE_")

	app_specific.ConfigExtension = ".tpc"

	// set the command pre and post hooks
	cmdconfig.CustomPreRunHook = preRunHook
	cmdconfig.CustomPostRunHook = postRunHook

	// Version check
	app_specific.VersionCheckHost = "hub.tailpipe.io"
	app_specific.VersionCheckPath = "api/cli/version/latest"

	// OciInstaller
	app_specific.DefaultImageRepoActualURL = "ghcr.io/turbot/tailpipe"
	app_specific.DefaultImageRepoDisplayURL = "hub.tailpipe.io"

	// set the default install dir
	defaultInstallDir, err := files.Tildefy("~/.tailpipe")
	error_helpers.FailOnError(err)
	app_specific.DefaultInstallDir = defaultInstallDir

	// set the default pipes install dir
	defaultPipesInstallDir, err := files.Tildefy("~/.pipes")
	filepaths.PipesInstallDir = defaultPipesInstallDir
	error_helpers.FailOnError(err)

	// set the default config path
	globalConfigPath := filepath.Join(defaultInstallDir, "config")

	// check whether install-dir env has been set - if so, respect it
	if envInstallDir, ok := os.LookupEnv(app_specific.EnvInstallDir); ok {
		globalConfigPath = filepath.Join(envInstallDir, "config")
		app_specific.InstallDir = envInstallDir
	} else {
		/*
			NOTE:
			If InstallDir is settable outside of default & env var, need to add
			the following code to end of initGlobalConfig in init.go
			app_specific.InstallDir = viper.GetString(constants.ArgInstallDir) at end of
		*/
		app_specific.InstallDir = defaultInstallDir
	}
	app_specific.DefaultConfigPath = strings.Join([]string{".", globalConfigPath}, ":")
}
