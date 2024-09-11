package cmdconfig

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/filepaths"
	"github.com/turbot/pipe-fittings/task"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/pipe-fittings/workspace_profile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/logger"
	"github.com/turbot/tailpipe/internal/parse"
)

var waitForTasksChannel chan struct{}
var tasksCancelFn context.CancelFunc

// postRunHook is a function that is executed before the PreRun of every command handler
func preRunHook(cmd *cobra.Command, args []string) error {
	utils.LogTime("cmdhook.preRunHook start")
	defer utils.LogTime("cmdhook.preRunHook end")

	viper.Set(constants.ConfigKeyActiveCommand, cmd)
	viper.Set(constants.ConfigKeyActiveCommandArgs, args)
	viper.Set(constants.ConfigKeyIsTerminalTTY, isatty.IsTerminal(os.Stdout.Fd()))

	ctx := cmd.Context()

	// set up the global viper config with default values from
	// config files and ENV variables
	ew := initGlobalConfig(ctx)
	// display any warnings
	ew.ShowWarnings()
	// TODO #errors sort exit code
	// check for error
	error_helpers.FailOnError(ew.Error)

	logger.Initialize()

	// runScheduledTasks skips running tasks if this instance is the plugin manager
	waitForTasksChannel = runScheduledTasks(ctx, cmd, args)

	// set the max memory if specified
	setMemoryLimit()
	return nil
}

// postRunHook is a function that is executed after the PostRun of every command handler
func postRunHook(_ *cobra.Command, _ []string) error {
	utils.LogTime("cmdhook.postRunHook start")
	defer utils.LogTime("cmdhook.postRunHook end")

	if waitForTasksChannel != nil {
		// wait for the async tasks to finish
		select {
		case <-time.After(100 * time.Millisecond):
			tasksCancelFn()
			return nil
		case <-waitForTasksChannel:
			return nil
		}
	}
	return nil
}

func setMemoryLimit() {
	maxMemoryBytes := viper.GetInt64(constants.ArgMemoryMaxMb) * 1024 * 1024
	if maxMemoryBytes > 0 {
		// set the max memory
		debug.SetMemoryLimit(maxMemoryBytes)
	}
}

// runScheduledTasks runs the task runner and returns a channel which is closed when
// task run is complete
//
// runScheduledTasks skips running tasks if this instance is the plugin manager
func runScheduledTasks(ctx context.Context, cmd *cobra.Command, args []string) chan struct{} {
	updateCheck := viper.GetBool(constants.ArgUpdateCheck)
	// for now the only scheduled task we support is update check so if that is disabled, do nothing
	if !updateCheck {
		return nil
	}

	taskUpdateCtx, cancelFn := context.WithCancel(ctx)
	tasksCancelFn = cancelFn

	return task.RunTasks(
		taskUpdateCtx,
		cmd,
		args,
		// pass the config value in rather than runRasks querying viper directly - to avoid concurrent map access issues
		// (we can use the update-check viper config here, since initGlobalConfig has already set it up
		// with values from the config files and ENV settings - update-check cannot be set from the command line)
		task.WithUpdateCheck(updateCheck),
	)
}

// initConfig reads in config file and ENV variables if set.
func initGlobalConfig(ctx context.Context) error_helpers.ErrorAndWarnings {
	utils.LogTime("cmdconfig.initGlobalConfig start")
	defer utils.LogTime("cmdconfig.initGlobalConfig end")

	// ensure config folders exist
	filepaths.EnsureConfigDir()

	// load workspace profile from the configured install dir
	loader, err := cmdconfig.GetWorkspaceProfileLoader[*workspace_profile.TpWorkspaceProfile]()
	error_helpers.FailOnError(err)

	config.GlobalWorkspaceProfile = loader.GetActiveWorkspaceProfile()
	// create the required data and internal folder for this workspace if needed
	err = config.GlobalWorkspaceProfile.EnsureWorkspaceDirs()
	error_helpers.FailOnError(err)

	// ensure we have a database file for this workspace
	err = database.EnsureDatabaseFile()
	error_helpers.FailOnError(err)

	var cmd = viper.Get(constants.ConfigKeyActiveCommand).(*cobra.Command)

	// set-up viper with defaults from the env and default workspace profile
	cmdconfig.BootstrapViper(loader, cmd, cmdconfig.WithConfigDefaults(configDefaults(cmd)), cmdconfig.WithDirectoryEnvMappings(dirEnvMappings()))

	// set the rest of the defaults from ENV
	// ENV takes precedence over any default configuration
	cmdconfig.SetDefaultsFromEnv(envMappings())

	// if an explicit workspace profile was set, add to viper as highest precedence default
	if loader.ConfiguredProfile != nil {
		cmdconfig.SetDefaultsFromConfig(loader.ConfiguredProfile.ConfigMap(cmd))
	}

	// load the connection config and HCL options
	tailpipeConfig, loadConfigErrorsAndWarnings := parse.LoadTailpipeConfig(ctx)
	if loadConfigErrorsAndWarnings.Error != nil {
		return loadConfigErrorsAndWarnings
	}

	// store global config
	config.GlobalConfig = tailpipeConfig

	// now validate all config values have appropriate values
	return validateConfig()
}

// now validate  config values have appropriate values
// (currently validates telemetry)
func validateConfig() error_helpers.ErrorAndWarnings {
	var res = error_helpers.ErrorAndWarnings{}
	telemetry := viper.GetString(constants.ArgTelemetry)
	if !helpers.StringSliceContains(constants.TelemetryLevels, telemetry) {
		res.Error = fmt.Errorf(`invalid value of 'telemetry' (%s), must be one of: %s`, telemetry, strings.Join(constants.TelemetryLevels, ", "))
		return res
	}

	return res
}
