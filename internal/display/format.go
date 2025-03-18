package display

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"

	"golang.org/x/exp/slices"
)

type FormatResource struct {
	formats.FormatDescription
}

func NewFormatResource(format *formats.FormatDescription) *FormatResource {
	return &FormatResource{
		FormatDescription: *format,
	}
}

// GetListData implements the printers.Listable interface
func (r *FormatResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("TYPE", r.Type),
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("DESCRIPTION", r.Description),
	)
	return res
}

// GetShowData implements the printers.Showable interface
func (r *FormatResource) GetShowData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("Type", r.Type),
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
		printers.NewFieldValue("Regex", r.Regex),
		printers.NewFieldValue("Properties", r.Properties),
	)
	return res
}

func ListFormatResources(ctx context.Context) ([]*FormatResource, error) {
	// map so that custom formats can override built-in format presets
	formatMap := make(map[string]*FormatResource)

	// add preset formats
	pm := plugin.NewPluginManager()
	defer pm.Close()
	for _, pluginVersion := range config.GlobalConfig.PluginVersions {
		if _, hasPresets := pluginVersion.Metadata["format_presets"]; hasPresets {
			desc, err := pm.Describe(ctx, pluginVersion.Name)
			if err != nil {
				return nil, err
			}
			for k, f := range desc.FormatPresets {
				formatMap[k] = NewFormatResource(f)
			}
		}
	}

	// add custom formats
	for _, f := range config.GlobalConfig.Formats {
		formatMap[f.GetUnqualifiedName()] = NewFormatResource(&formats.FormatDescription{
			Name:        f.ShortName,
			Type:        f.Type,
			Description: types.SafeString(f.Description),
		})
	}

	// slice of unique formats
	var out []*FormatResource
	for _, format := range formatMap {
		out = append(out, format)
	}

	// sort by type then name
	slices.SortFunc(out, func(a, b *FormatResource) int {
		if a.Type != b.Type {
			return strings.Compare(a.Type, b.Type)
		}
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

func GetFormatResource(ctx context.Context, name string) (*FormatResource, error) {
	// format should be specified as `type.instance` - validate we have 2 parts
	parts := strings.Split(name, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("format should be specified as `type.instance`")
	}

	formatType := parts[0]
	// formatInstance := parts[1]

	// get plugin for type
	var pluginName string
	if pn, ok := config.GetPluginForFormatType(formatType, config.GlobalConfig.PluginVersions); !ok {
		return nil, fmt.Errorf("no plugin found for type '%s'", formatType)
	} else {
		pluginName = pn
	}

	// determine if custom format
	var customFormats []*config.Format
	if customFormat, isCustom := config.GlobalConfig.Formats[name]; isCustom {
		customFormats = append(customFormats, customFormat)
	}

	// describe plugin to get format info, if custom we need to pass it in
	pm := plugin.NewPluginManager()
	defer pm.Close()
	desc, err := pm.Describe(ctx, pluginName, customFormats...)
	if err != nil {
		return nil, fmt.Errorf("failed to describe format '%s': %w", name, err)
	}

	// prefer custom format (order of precedence)
	if format, isCustom := desc.CustomFormats[name]; isCustom {
		return NewFormatResource(format), nil
	}

	// if format is a preset
	if format, isPreset := desc.FormatPresets[name]; isPreset {
		return NewFormatResource(format), nil
	}

	return nil, fmt.Errorf("format '%s' not found", name)
}
