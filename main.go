package main

import (
	"context"
	"os"

	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/cmd"
	"github.com/turbot/tailpipe/internal/cmdconfig"
	localconstants "github.com/turbot/tailpipe/internal/constants"
)

var exitCode int

var (
	// These variables will be set by GoReleaser.
	version = localconstants.DefaultVersion
	commit  = localconstants.DefaultCommit
	date    = localconstants.DefaultDate
	builtBy = localconstants.DefaultBuiltBy
)

func main() {
	ctx := context.Background()
	utils.LogTime("main start")

	// add the auto-populated version properties into viper.
	setVersionProperties()

	defer func() {
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			if exitCode == 0 {
				exitCode = 255
			}
		}
		utils.LogTime("main end")
		utils.DisplayProfileData(os.Stderr)
		os.Exit(exitCode)
	}()

	cmdconfig.SetAppSpecificConstants()

	// execute the root command
	exitCode = cmd.Execute()
}

func setVersionProperties() {
	viper.SetDefault(constants.ConfigKeyVersion, version)
	viper.SetDefault(constants.ConfigKeyCommit, commit)
	viper.SetDefault(constants.ConfigKeyDate, date)
	viper.SetDefault(constants.ConfigKeyBuiltBy, builtBy)
}
