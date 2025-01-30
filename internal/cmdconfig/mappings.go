package cmdconfig

import (
	"github.com/spf13/cobra"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/constants"
	"golang.org/x/exp/maps"
)

func configDefaults(cmd *cobra.Command) map[string]any {
	defs := map[string]any{
		// global general options
		pconstants.ArgUpdateCheck: true,

		// memory
		pconstants.ArgMemoryMaxMb: 1024,
	}

	cmdSpecificDefaults, ok := cmdSpecificDefaults()[cmd.Name()]
	if ok {
		maps.Copy(defs, cmdSpecificDefaults)
	}
	return defs
}

// command specific config defaults (keyed by comand name)
func cmdSpecificDefaults() map[string]map[string]any {
	return map[string]map[string]any{
		"server": {
			pconstants.ArgEnvironment: "release",
		},
		"query": {
			pconstants.ArgAutoComplete: true,
		},
	}
}

// environment variable mappings for directory paths which must be set as part of the viper bootstrap process
func dirEnvMappings() map[string]cmdconfig.EnvMapping {
	return map[string]cmdconfig.EnvMapping{}
}

// a map of known environment variables to map to viper keys - these are set as part of InitGlobalConfig
func envMappings() map[string]cmdconfig.EnvMapping {
	return map[string]cmdconfig.EnvMapping{
		app_specific.EnvUpdateCheck:       {ConfigVar: []string{pconstants.ArgUpdateCheck}, VarType: cmdconfig.EnvVarTypeBool},
		app_specific.EnvMemoryMaxMb:       {ConfigVar: []string{pconstants.ArgMemoryMaxMb}, VarType: cmdconfig.EnvVarTypeInt},
		app_specific.EnvConfigPath:        {ConfigVar: []string{pconstants.ArgConfigPath}, VarType: cmdconfig.EnvVarTypeString},
		app_specific.EnvQueryTimeout:      {ConfigVar: []string{pconstants.ArgDatabaseQueryTimeout}, VarType: cmdconfig.EnvVarTypeInt},
		app_specific.EnvMemoryMaxMbPlugin: {ConfigVar: []string{pconstants.ArgMemoryMaxMbPlugin}, VarType: cmdconfig.EnvVarTypeInt},
		constants.EnvPluginStartTimeout:   {ConfigVar: []string{pconstants.ArgPluginStartTimeout}, VarType: cmdconfig.EnvVarTypeInt},
	}
}
