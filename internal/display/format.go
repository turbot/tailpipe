package display

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	sdktypes "github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"

	"golang.org/x/exp/slices"
)

type FormatResource struct {
	sdktypes.FormatDescription
}

func NewFormatResource(format *sdktypes.FormatDescription) *FormatResource {
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
	var fields []printers.FieldValue
	// default fields
	fields = append(fields,
		printers.NewFieldValue("Type", r.Type),
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
		printers.NewFieldValue("Regex", r.Regex),
	)

	// fields from properties
	for key, value := range r.Properties {
		fields = append(fields, printers.NewFieldValue(utils.ToTitleCase(key), value))
	}

	return printers.NewRowData(fields...)
}

func ListFormatResources(ctx context.Context) ([]*FormatResource, error) {
	// map so that custom formats can override built-in format presets
	formatMap := make(map[string]*FormatResource)

	// add preset formats and types from plugins
	pm := plugin.NewPluginManager()
	defer pm.Close()
	for _, pluginVersion := range config.GlobalConfig.PluginVersions {
		if pluginVersion.Metadata != nil {
			desc, err := pm.Describe(ctx, pluginVersion.Name)
			if err != nil {
				return nil, err
			}

			// populate types
			for _, t := range desc.FormatTypes {
				// types do not have an instance name or description
				formatMap[t] = NewFormatResource(&sdktypes.FormatDescription{Type: t})
			}

			// populate presets
			for k, f := range desc.FormatPresets {
				formatMap[k] = NewFormatResource(f)
			}
		}
	}

	// add custom formats
	for _, f := range config.GlobalConfig.Formats {
		formatMap[f.GetUnqualifiedName()] = NewFormatResource(&sdktypes.FormatDescription{
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
	switch len(parts) {
	case 1:
		// assume type
		return getFormatType(ctx, name)
	case 2:
		// assume type.instance
		return getFormatInstance(ctx, name)
	default:
		return nil, fmt.Errorf("format should be specified as `type` or `type.instance`")
	}
}

func getFormatType(_ context.Context, formatType string) (*FormatResource, error) {
	// Check this is a valid type exported by a plugin
	if _, ok := config.GetPluginForFormatType(formatType, config.GlobalConfig.PluginVersions); ok {
		// TODO: #graza once we add source of format add plugin name here https://github.com/turbot/tailpipe/issues/283
		return NewFormatResource(&sdktypes.FormatDescription{Type: formatType}), nil
	}
	// if not found, return error
	return nil, fmt.Errorf("no plugin found for type '%s'", formatType)
}

func getFormatInstance(ctx context.Context, name string) (*FormatResource, error) {
	formatType := strings.Split(name, ".")[0]

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
	desc, err := pm.Describe(ctx, pluginName, plugin.WithCustomFormats(customFormats...))
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
