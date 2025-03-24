package parse

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/turbot/tailpipe/internal/config"
)

// ParseBuiltInSdkFormat tells the SDK to parse the format config for a built in format.
func ParseBuiltInSdkFormat(format *config.Format) (formats.Format, error) {
	// there must not be a preset
	if format.PresetName != "" {
		return nil, fmt.Errorf("ParseBuiltInSdkFormat cannot be used with preset formats")
	}
	// if must be a built-in format
	if _, ok := formats.BuiltInFormats[format.FullName]; !ok {
		return nil, fmt.Errorf("ParseBuiltInSdkFormat cannot be used with non-built-in formats")
	}

	// we need to parse the format config to populate the format
	formatConfigData := &types.FormatConfigData{
		ConfigDataImpl: &types.ConfigDataImpl{
			Hcl:   format.Config.Hcl,
			Range: format.Config.Range.HclRange(),
		},
		Name: format.FullName,
	}
	// parse the format config, passing the built-in formats as the format ctor map
	parsedFormat, err := formats.ParseFormat(formatConfigData, formats.BuiltInFormats)
	if err != nil {
		return nil, fmt.Errorf("error parsing built-in format %s: %w", format.FullName, err)
	}
	return parsedFormat, nil
}
