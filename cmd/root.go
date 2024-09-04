package cmd

import (
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/utils"
	localconstants "github.com/turbot/tailpipe/internal/constants"
)

var exitCode int

// Build the cobra command that handles our command line tool.
func rootCommand() *cobra.Command {
	// Define our command
	rootCmd := &cobra.Command{
		Use:     "tailpipe [--version] [--help] COMMAND [args]",
		Short:   localconstants.TailpipeShortDescription,
		Long:    localconstants.TailpipeLongDescription,
		Version: viper.GetString("main.version"),
		Run: func(cmd *cobra.Command, args []string) {
			err := cmd.Help()
			error_helpers.FailOnError(err)
		},
	}

	utils.LogTime("cmd.root.InitCmd start")
	defer utils.LogTime("cmd.root.InitCmd end")

	rootCmd.SetVersionTemplate("Tailpipe v{{.Version}}\n")

	// TODO #config think about default config path
	defaultConfigPath, err := filepath.Abs("")
	error_helpers.FailOnError(err)

	cmdconfig.
		OnCmd(rootCmd).
		AddPersistentStringFlag(constants.ArgConfigPath, defaultConfigPath, "The location to search for config files")

	rootCmd.AddCommand(
		collectCmd(),
		pluginCmd(),
	)

	// disable auto completion generation, since we don't want to support
	// powershell yet - and there's no way to disable powershell in the default generator
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	return rootCmd
}

func Execute() int {
	rootCmd := rootCommand()
	utils.LogTime("cmd.root.Execute start")
	defer utils.LogTime("cmd.root.Execute end")

	if err := rootCmd.Execute(); err != nil {
		exitCode = -1
	}
	return exitCode
}
