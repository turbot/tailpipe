package cmdconfig

import (
	"github.com/spf13/cobra"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"golang.org/x/exp/maps"
)

func configDefaults(cmd *cobra.Command) map[string]any {
	defs := map[string]any{
		// global general options
		constants.ArgTelemetry:   constants.TelemetryInfo,
		constants.ArgUpdateCheck: true,

		// memory
		constants.ArgMemoryMaxMb: 1024,
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
			constants.ArgEnvironment: "release",
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
		app_specific.EnvTelemetry:         {ConfigVar: []string{constants.ArgTelemetry}, VarType: cmdconfig.EnvVarTypeString},
		app_specific.EnvUpdateCheck:       {ConfigVar: []string{constants.ArgUpdateCheck}, VarType: cmdconfig.EnvVarTypeBool},
		app_specific.EnvMemoryMaxMb:       {ConfigVar: []string{constants.ArgMemoryMaxMb}, VarType: cmdconfig.EnvVarTypeInt},
		app_specific.EnvMemoryMaxMbPlugin: {ConfigVar: []string{constants.ArgMemoryMaxMbPlugin}, VarType: cmdconfig.EnvVarTypeInt},
		app_specific.EnvConfigPath:        {ConfigVar: []string{constants.ArgConfigPath}, VarType: cmdconfig.EnvVarTypeString},
	}
}
