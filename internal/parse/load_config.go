package parse

import (
	"fmt"
	"log/slog"

	filehelpers "github.com/turbot/go-kit/files"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/parse"
	"github.com/turbot/tailpipe/internal/config"
)

func LoadTailpipeConfig(configPath string) (_ *config.TailpipeConfig, err error) {
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
	if diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}

	// parse the files
	body, diags := parse.ParseHclFiles(fileData)
	if diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}
	content, diags := body.Content(config.ConfigBlockSchema)
	if diags.HasErrors() {
		return nil, error_helpers.HclDiagsToError("Failed to parse config", diags)
	}

	// create parse context for the decode
	parseCtx := NewConfigParseContext(configPath)
	parseCtx.SetDecodeContent(content, fileData)

	// now decode
	// we may need to decode more than once as we gather dependencies as we go
	// continue decoding as long as the number of unresolved blocks decreases
	prevUnresolvedBlocks := 0
	var config *config.TailpipeConfig

	for attempts := 0; ; attempts++ {
		config, diags = decode(parseCtx)
		if diags.HasErrors() {
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

	return config, nil

}
