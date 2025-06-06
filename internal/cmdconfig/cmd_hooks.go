package cmdconfig

import (
	"context"
	"log/slog"
	"os"
	"runtime/debug"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/filepaths"
	pparse "github.com/turbot/pipe-fittings/v2/parse"
	"github.com/turbot/pipe-fittings/v2/task"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/pipe-fittings/v2/workspace_profile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/logger"
	"github.com/turbot/tailpipe/internal/parse"
	"github.com/turbot/tailpipe/internal/plugin"
)

var waitForTasksChannel chan struct{}
var tasksCancelFn context.CancelFunc

// postRunHook is a function that is executed before the PreRun of every command handler
func preRunHook(cmd *cobra.Command, args []string) error {
	utils.LogTime("cmdhook.preRunHook start")
	defer utils.LogTime("cmdhook.preRunHook end")

	viper.Set(pconstants.ConfigKeyActiveCommand, cmd)
	viper.Set(pconstants.ConfigKeyActiveCommandArgs, args)
	viper.Set(pconstants.ConfigKeyIsTerminalTTY, isatty.IsTerminal(os.Stdout.Fd()))

	ctx := cmd.Context()
	logger.Initialize()

	// set up the global viper config with default values from
	// config files and ENV variables
	ew := initGlobalConfig(ctx)
	// display any warnings
	ew.ShowWarnings()
	// TODO #errors sort exit code  https://github.com/turbot/tailpipe/issues/106
	// check for error
	error_helpers.FailOnError(ew.Error)

	// pump in the initial set of logs (AFTER we have loaded the config, which may specify log level)
	displayStartupLog()

	// runScheduledTasks skips running tasks if this instance is the plugin manager
	waitForTasksChannel = runScheduledTasks(ctx, cmd, args)

	// set the max memory if specified
	setMemoryLimit()

	return nil
}

func displayStartupLog() {
	slog.Info("Tailpipe CLI",
		"app version", viper.GetString("main.version"),
		"log level", os.Getenv(app_specific.EnvLogLevel))

	// log resource limits
	slog.Info("Resource limits",
		"max CLI memory (mb)", viper.GetInt64(pconstants.ArgMemoryMaxMb),
		"max plugin memory (mb)", viper.GetInt64(pconstants.ArgMemoryMaxMbPlugin),
		"max temp dir size (mb)", viper.GetInt64(pconstants.ArgTempDirMaxMb))
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
	maxMemoryBytes := viper.GetInt64(pconstants.ArgMemoryMaxMb) * 1024 * 1024
	if maxMemoryBytes > 0 {
		slog.Info("Setting CLI memory limit", "max memory (mb)", maxMemoryBytes/(1024*1024))
		// set the max memory
		debug.SetMemoryLimit(maxMemoryBytes)
	}
}

// runScheduledTasks runs the task runner and returns a channel which is closed when
// task run is complete
//
// runScheduledTasks skips running tasks if this instance is the plugin manager
func runScheduledTasks(ctx context.Context, cmd *cobra.Command, args []string) chan struct{} {
	updateCheck := viper.GetBool(pconstants.ArgUpdateCheck)
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

	// define parse opts to disable hcl template parsing for properties which will have a grok pattern
	parseOpts := []pparse.ParseHclOpt{
		// legacy auto-escaping of 'file_layout' property
		pparse.WithDisableTemplateForProperties(constants.GrokConfigProperties),
		// escape properties within backticks
		pparse.WithEscapeBackticks(true),
	}
	// load workspace profile from the configured install dir
	loader, err := cmdconfig.GetWorkspaceProfileLoader[*workspace_profile.TailpipeWorkspaceProfile](parseOpts...)
	if err != nil {
		return error_helpers.NewErrorsAndWarning(err)
	}

	config.GlobalWorkspaceProfile = loader.GetActiveWorkspaceProfile()
	// create the required data and internal folder for this workspace if needed
	err = config.GlobalWorkspaceProfile.EnsureWorkspaceDirs()
	if err != nil {
		return error_helpers.NewErrorsAndWarning(err)
	}

	// ensure we have a database file for this workspace
	err = database.EnsureDatabaseFile(ctx)
	if err != nil {
		return error_helpers.NewErrorsAndWarning(err)
	}

	var cmd = viper.Get(pconstants.ConfigKeyActiveCommand).(*cobra.Command)

	// set-up viper with defaults from the env and default workspace profile
	cmdconfig.BootstrapViper(loader, cmd, cmdconfig.WithConfigDefaults(configDefaults(cmd)), cmdconfig.WithDirectoryEnvMappings(dirEnvMappings()))

	// set the rest of the defaults from ENV
	// ENV takes precedence over any default configuration
	cmdconfig.SetDefaultsFromEnv(envMappings())

	// if an explicit workspace profile was set, add to viper as highest precedence default
	if loader.ConfiguredProfile != nil {
		cmdconfig.SetDefaultsFromConfig(loader.ConfiguredProfile.ConfigMap(cmd))
	}

	// ensure the core plugin is installed or the min version requirement is satisfied
	// NOTE: if this installed the core plugin, the plugin version file will be updated and the updated file returned
	pluginVersionFile, err := plugin.EnsureCorePlugin(ctx)
	if err != nil {
		return error_helpers.NewErrorsAndWarning(err)
	}

	// load the connection config and HCL options (passing plugin versions
	tailpipeConfig, loadConfigErrorsAndWarnings := parse.LoadTailpipeConfig(pluginVersionFile)

	if loadConfigErrorsAndWarnings.Error != nil {
		return loadConfigErrorsAndWarnings
	}

	if loadConfigErrorsAndWarnings.Warnings != nil {
		for _, warning := range loadConfigErrorsAndWarnings.Warnings {
			error_helpers.ShowWarning(warning)
		}
	}
	// store global config
	config.GlobalConfig = tailpipeConfig

	// now validate all config values have appropriate values
	return validateConfig()
}

// now validate  config values have appropriate values
func validateConfig() error_helpers.ErrorAndWarnings {
	var res = error_helpers.ErrorAndWarnings{}

	// TODO #config validate

	return res
}
