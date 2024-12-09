package parse

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/gertd/go-pluralize"
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

	//// load connections and  config from the installation folder -  load all spc files from config directory
	//include := filehelpers.InclusionsFromExtensions(constants.ConnectionConfigExtensions)
	//loadOptions := &loadConfigOptions{include: include}
	//ew = loadConfig(ctx, filepaths.EnsureConfigDir(), tailpipeConfig, loadOptions)
	//if ew.GetError() != nil {
	//	return nil, ew
	//}
	//// merge the warning from this call
	//errorsAndWarnings.AddWarning(ew.Warnings...)
	//
	//// now validate the config
	warnings, errors := tailpipeConfig.Validate()
	logValidationResult(warnings, errors)

	return tailpipeConfig, errorsAndWarnings
}

func logValidationResult(warnings []string, errors []string) {
	if len(warnings) > 0 {
		error_helpers.ShowWarning(buildValidationLogString(warnings, "warning"))
		log.Printf("[TRACE] %s", buildValidationLogString(warnings, "warning"))
	}
	if len(errors) > 0 {
		error_helpers.ShowWarning(buildValidationLogString(errors, "error"))
		log.Printf("[TRACE] %s", buildValidationLogString(errors, "error"))
	}
}

func buildValidationLogString(items []string, validationType string) string {
	count := len(items)
	if count == 0 {
		return ""
	}
	var str strings.Builder
	str.WriteString(fmt.Sprintf("connection config has has %d validation %s:\n",
		count,
		pluralize.NewClient().Pluralize(validationType, count, false),
	))
	for _, w := range items {
		str.WriteString(fmt.Sprintf("\t %s\n", w))
	}
	return str.String()
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
	body, diags := parse.ParseHclFiles(fileData)
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
	var tailpipeConfig *config.TailpipeConfig

	for attempts := 0; ; attempts++ {
		tailpipeConfig, diags = decodeTailpipeConfig(parseCtx)
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

	return tailpipeConfig, nil

}
