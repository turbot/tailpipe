package parse

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hashicorp/hcl/v2"
	"github.com/spf13/viper"
	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/parse"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
)

// LoadTailpipeConfig loads the HCL connection config, resources and workspace profiles
func LoadTailpipeConfig(ctx context.Context) (tailpipeConfig *config.TailpipeConfig, ew error_helpers.ErrorAndWarnings) {
	utils.LogTime("TailpipeConfig.loadTailpipeConfig start")
	defer utils.LogTime("TailpipeConfig.loadTailpipeConfig end")

	defer func() {
		if r := recover(); r != nil {
			ew = error_helpers.NewErrorsAndWarning(helpers.ToError(r))
		}
	}()

	// load the tailpipe config
	tailpipeConfig, ew = parseTailpipeConfig(viper.GetString(pconstants.ArgConfigPath))
	if ew.Error != nil {
		return nil, ew
	}

	// load plugin versions
	v, err := versionfile.LoadPluginVersionFile(ctx)
	if err != nil {
		ew.Error = err
		return nil, ew
	}

	// TODO KAI CHECK THIS
	// add any "local" plugins (i.e. plugins installed under the 'local' folder) into the version file
	localPluginErrors := v.AddLocalPlugins(ctx)
	ew.Merge(localPluginErrors)
	if ew.Error != nil {
		return nil, ew
	}

	tailpipeConfig.PluginVersions = v.Plugins

	// initialise all partitions - this populates the Plugin and CustomTable (where set) properties
	tailpipeConfig.InitPartitions(v)

	// now validate the config
	diags := tailpipeConfig.Validate()
	if diags != nil && diags.HasErrors() {
		ew.Error = error_helpers.HclDiagsToError("config validation failed", diags)
	}
	// merge in any warnings
	ew.Warnings = append(ew.Warnings, error_helpers.HclDiagsToWarnings(diags)...)

	return tailpipeConfig, ew
}

// load config from the given folder and update TailpipeConfig
// NOTE: this mutates steampipe config

func parseTailpipeConfig(configPath string) (_ *config.TailpipeConfig, ew error_helpers.ErrorAndWarnings) {
	defer func() {
		if r := recover(); r != nil {
			if ew.Error == nil {
				ew.Error = helpers.ToError(r)
			}
		}
	}()

	var diags hcl.Diagnostics
	var res = config.NewTailpipeConfig()

	// find files in target folder only (non-recursive)
	configPaths, err := filehelpers.ListFiles(configPath, &filehelpers.ListOptions{
		Flags:   filehelpers.FilesFlat,
		Include: filehelpers.InclusionsFromExtensions([]string{app_specific.ConfigExtension}),
	})
	if err != nil {
		return nil, error_helpers.NewErrorsAndWarning(err)
	}
	if len(configPaths) == 0 {
		return res, ew
	}

	// load the file data
	fileData, moreDiags := parse.LoadFileData(configPaths...)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return nil, error_helpers.DiagsToErrorsAndWarnings("Failed to parse config", diags)
	}

	// first apply escaping - if properties have values surrounded in backticks, we need to escape them
	// we also respected the legacy auto-escaping mechanism for specific properties

	// define parse opts to disable hcl template parsing for properties which will have a grok pattern
	parseOpts := []parse.ParseHclOpt{
		// legacy auto-escaping of 'file_layout' property
		parse.WithDisableTemplateForProperties(constants.GrokConfigProperties),
		// escape properties within backticks
		parse.WithEscapeBackticks(true),
	}
	fileData, moreDiags = parse.ApplyPropertyEscaping(fileData, parseOpts...)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return nil, error_helpers.DiagsToErrorsAndWarnings("Failed to parse config", diags)
	}

	// now parse teh file data
	body, moreDiags := parse.ParseHclFiles(fileData)
	diags = append(diags, moreDiags...)

	if diags.HasErrors() {
		return nil, error_helpers.DiagsToErrorsAndWarnings("Failed to parse config", diags)
	}
	content, moreDiags := body.Content(parse.TailpipeConfigBlockSchema)
	diags = append(diags, moreDiags...)
	if diags.HasErrors() {
		return nil, error_helpers.DiagsToErrorsAndWarnings("Failed to parse config", diags)
	}
	// convert diags to errors and warnings to capture any warnings
	ew.Warnings = error_helpers.HclDiagsToWarnings(diags)

	// create parse context for the decode
	parseCtx, err := NewConfigParseContext(configPath)
	if err != nil {
		ew.Error = err
		return nil, ew
	}
	parseCtx.SetDecodeContent(content, fileData)

	// now decode
	// we may need to decode more than once as we gather dependencies as we go
	// continue decoding as long as the number of unresolved blocks decreases
	prevUnresolvedBlocks := 0

	for attempts := 0; ; attempts++ {
		moreDiags = decodeTailpipeConfig(parseCtx)
		diags = append(diags, moreDiags...)
		if diags.HasErrors() {
			ew.Error = error_helpers.HclDiagsToError("Failed to decode all config files", diags)
			return nil, ew
		}

		// if there are no unresolved blocks, we are done
		unresolvedBlocks := len(parseCtx.UnresolvedBlocks)
		if unresolvedBlocks == 0 {
			slog.Debug("workspace profile parse complete with no unresolved blocks", "decode passes", attempts+1)
			break
		}
		// if the number of unresolved blocks has NOT reduced, fail
		if prevUnresolvedBlocks != 0 && unresolvedBlocks >= prevUnresolvedBlocks {
			// so all dependencies have been resolved that we are able
			// do one further pass where we try to resolve format s
			// we do this at the end to be sure that if a format preset is overridden by a format in the config,
			// we correctly resolve the format in the config
			if !parseCtx.resolveFormatPresets {
				// set the resolveFormatPresets so that the next decode round will resolve format presets
				parseCtx.resolveFormatPresets = true
			} else {
				// we have already tried to resolve format presets and still have dependency errors
				str := parseCtx.FormatDependencies()
				ew.Error = fmt.Errorf("failed to resolve config dependencies after %d attempts\nDependencies:\n%s", attempts+1, str)
				return nil, ew
			}

		}
		// update prevUnresolvedBlocks
		prevUnresolvedBlocks = unresolvedBlocks
	}

	return parseCtx.tailpipeConfig, ew

}
