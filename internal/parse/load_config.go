package parse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/viper"
	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/pipe-fittings/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	sdkconstants "github.com/turbot/tailpipe/internal/constants"
)

// LoadTailpipeConfig loads the HCL connection config, resources and workspace profiles
func LoadTailpipeConfig(ctx context.Context) (tailpipeConfig *config.TailpipeConfig, errorsAndWarnings error_helpers.ErrorAndWarnings) {
	utils.LogTime("TailpipeConfig.loadTailpipeConfig start")
	defer utils.LogTime("TailpipeConfig.loadTailpipeConfig end")

	defer func() {
		if r := recover(); r != nil {
			errorsAndWarnings = error_helpers.NewErrorsAndWarning(helpers.ToError(r))
		}
	}()

	// load the tailpipe config
	tailpipeConfig, err := parseTailpipeConfig(viper.GetString(constants.ArgConfigPath))
	if err != nil {
		return nil, error_helpers.NewErrorsAndWarning(err)
	}

	// load plugin versions
	v, err := versionfile.LoadPluginVersionFile(ctx)
	if err != nil {
		return nil, error_helpers.NewErrorsAndWarning(err)
	}

	// add any "local" plugins (i.e. plugins installed under the 'local' folder) into the version file
	ew := v.AddLocalPlugins(ctx)
	if ew.GetError() != nil {
		return nil, ew
	}
	tailpipeConfig.PluginVersions = v.Plugins

	// initialise all partitions - this populates the Plugin and CustomTable (where set) properties
	tailpipeConfig.InitPartitions()

	// now validate the config
	ew.Error = tailpipeConfig.Validate()

	return tailpipeConfig, errorsAndWarnings
}

// load config from the given folder and update TailpipeConfig
// NOTE: this mutates steampipe config

func parseTailpipeConfig(configPath string) (_ *config.TailpipeConfig, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	var res = config.NewTailpipeConfig()

	// find files in target folder only (non-recursive)
	configPaths, err := filehelpers.ListFiles(configPath, &filehelpers.ListOptions{
		Flags:   filehelpers.FilesFlat,
		Include: filehelpers.InclusionsFromExtensions([]string{app_specific.ConfigExtension}),
	})
	if err != nil {
		return nil, err
	}
	if len(configPaths) == 0 {
		return res, nil
	}

	// load the file data
	fileData, diags := parse.LoadFileData(configPaths...)
	if diags != nil && diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}

	// parse the files
	// define parse opts to disable hcl template parsing for properties which will have a grok pattern
	parseOpts := []parse.ParseHclOpt{
		parse.WithDisableTemplateForProperties(sdkconstants.GrokConfigProperties),
	}

	//
	body, diags := parse.ParseHclFiles(fileData, parseOpts...)
	if diags != nil && diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}
	content, diags := body.Content(parse.TpConfigBlockSchema)
	if diags != nil && diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}

	// create parse context for the decode
	parseCtx := NewConfigParseContext(configPath)
	parseCtx.SetDecodeContent(content, fileData)

	// now decode
	// we may need to decode more than once as we gather dependencies as we go
	// continue decoding as long as the number of unresolved blocks decreases
	prevUnresolvedBlocks := 0

	for attempts := 0; ; attempts++ {
		diags = decodeTailpipeConfig(parseCtx)
		if diags != nil && diags.HasErrors() {
			return nil, error_helpers.HclDiagsToError("Failed to decode all config files", diags)
		}

		// if there are no unresolved blocks, we are done
		unresolvedBlocks := len(parseCtx.UnresolvedBlocks)
		if unresolvedBlocks == 0 {
			slog.Debug("workspace profile parse complete with no unresolved blocks", "decode passes", attempts+1)
			break
		}
		// if the number of unresolved blocks has NOT reduced, fail
		if prevUnresolvedBlocks != 0 && unresolvedBlocks >= prevUnresolvedBlocks {
			str := parseCtx.FormatDependencies()
			return nil, fmt.Errorf("failed to resolve workspace profile dependencies after %d attempts\nDependencies:\n%s", attempts+1, str)
		}
		// update prevUnresolvedBlocks
		prevUnresolvedBlocks = unresolvedBlocks
	}

	return parseCtx.tailpipeConfig, nil

}
