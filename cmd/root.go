package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/constants"
)

var exitCode int

// Build the cobra command that handles our command line tool.
func rootCommand() *cobra.Command {
	// Define our command
	rootCmd := &cobra.Command{
		Use:     "tailpipe [--version] [--help] COMMAND [args]",
		Short:   constants.TailpipeShortDescription,
		Long:    constants.TailpipeLongDescription,
		Version: viper.GetString("main.version"),
		Run: func(cmd *cobra.Command, args []string) {
			err := cmd.Help()
			error_helpers.FailOnError(err)
		},
	}

	utils.LogTime("cmd.root.InitCmd start")
	defer utils.LogTime("cmd.root.InitCmd end")

	rootCmd.SetVersionTemplate("Tailpipe v{{.Version}}\n")

	defaultConfigPath := filepaths.EnsureConfigDir()

	cmdconfig.
		OnCmd(rootCmd).
		AddPersistentStringFlag(pconstants.ArgConfigPath, defaultConfigPath, "The location to search for config files").
		AddPersistentStringFlag(pconstants.ArgWorkspaceProfile, "default", "Sets the Tailpipe workspace profile")

	rootCmd.AddCommand(
		queryCmd(),
		collectCmd(),
		connectCmd(),
		pluginCmd(),
		compactCmd(),
		sourceCmd(),
		tableCmd(),
		partitionCmd(),
		formatCmd(),
	)

	// disable auto completion generation, since we don't want to support
	// powershell yet - and there's no way to disable powershell in the default generator
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	return rootCmd
}

func Execute() int {
	utils.LogTime("cmd.root.Execute start")
	defer utils.LogTime("cmd.root.Execute end")
	rootCmd := rootCommand()
	if err := rootCmd.Execute(); err != nil {
		exitCode = -1
	}
	return exitCode
}
