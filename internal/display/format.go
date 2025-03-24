package display

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/formats"
	sdktypes "github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/parse"
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
	// format should be specified as `type.instance` (for a format instance) or 'type' (for a format type)
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
	if _, ok := formats.BuiltInFormats[formatType]; ok {
		desc := &sdktypes.FormatDescription{
			Type: formatType,
			//Description: "Format type.",
			//Source: constants.BuiltIn
		}
		return NewFormatResource(desc), nil
	}
	// Check this is a valid type exported by a plugin
	_, ok := config.GetPluginForFormatType(formatType, config.GlobalConfig.PluginVersions)
	if !ok {
		// if not found, return error
		return nil, fmt.Errorf("no plugin found for type '%s'", formatType)
	}

	// TODO: #graza once we add source of format add plugin name here https://github.com/turbot/tailpipe/issues/283
	return NewFormatResource(&sdktypes.FormatDescription{Type: formatType}), nil

}

func getFormatInstance(ctx context.Context, name string) (*FormatResource, error) {

	// construct a list of custom formats - this will have zero or 1 items in it and we do it purely so we
	// can pass it as a variadic parameter to the Describe function
	var customFormats []*config.Format

	// if this is a custom format of a type implemented by the sdk, we can just describe it directly
	customFormat, ok := config.GlobalConfig.Formats[name]
	if ok {
		customFormats = append(customFormats, customFormat)
		// is this a built in format?
		if _, ok := formats.BuiltInFormats[customFormat.Type]; ok {
			// // parse the format
			parsedFormat, err := parse.ParseBuiltInSdkFormat(customFormat)
			if err != nil {
				return nil, fmt.Errorf("error parsing built-in format %s: %w", customFormat.FullName, err)
			}
			desc := formats.DescribeFormat(parsedFormat)
			return NewFormatResource(desc), nil
		}
	}

	// otherwise we find the plugin responsible for either defining this format or the plugin which

	// get the plugin which provides the format (note  config.GetPluginForFormatByName is smart enough
	// to check whether this format is a preset and if so, return the plugin which provides the preset)
	pluginName, ok := config.GetPluginForFormatByName(name)
	if !ok {
		return nil, fmt.Errorf("no plugin found for format '%s'", name)
	}

	// describe plugin to get format info, passing the custom formats list in case this is custom
	// (we will have determined this above)
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
