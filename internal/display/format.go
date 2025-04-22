package display

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/utils"
	sdktypes "github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
)

const formatTypeDescription = "This is a format type, it can be used for defining instances of formats."

type FormatResource struct {
	sdktypes.FormatDescription
	Location string `json:"location,omitempty"`
}

func NewFormatResource(location string, format *sdktypes.FormatDescription) *FormatResource {
	return &FormatResource{
		Location:          location,
		FormatDescription: *format,
	}
}

// GetListData implements the printers.Listable interface
func (r *FormatResource) GetListData() *printers.RowData {
	name := r.Name
	if name == "" {
		name = "-"
	}
	res := printers.NewRowData(
		printers.NewFieldValue("TYPE", r.Type),
		printers.NewFieldValue("NAME", name),
		printers.NewFieldValue("LOCATION", r.Location),
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
		printers.NewFieldValue("Location", r.Location),
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
				formatMap[t] = NewFormatResource(pluginVersion.Name, &sdktypes.FormatDescription{
					Type:        t,
					Description: formatTypeDescription,
				})
			}

			// populate presets
			for k, f := range desc.FormatPresets {
				formatMap[k] = NewFormatResource(pluginVersion.Name, f)
			}
		}
	}

	// add custom formats
	for _, f := range config.GlobalConfig.Formats {
		formatMap[f.GetUnqualifiedName()] = NewFormatResource(f.Config.Range.Filename, &sdktypes.FormatDescription{
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
	// Check this is a valid type exported by a plugin
	pluginName, ok := config.GetPluginForFormatType(formatType, config.GlobalConfig.PluginVersions)
	if !ok {
		// if not found, return error
		return nil, fmt.Errorf("no plugin found for type '%s'", formatType)
	}

	return NewFormatResource(pluginName, &sdktypes.FormatDescription{
		Type:        formatType,
		Description: formatTypeDescription,
	}), nil
}

func getFormatInstance(ctx context.Context, name string) (*FormatResource, error) {
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

	// if this is a custom format, pass the custom formats to the describe call
	var opts []plugin.DescribeOpts
	var filePath string
	if customFormat, ok := config.GlobalConfig.Formats[name]; ok {
		filePath = customFormat.Config.Range.Filename
		opts = append(opts, plugin.WithCustomFormats(customFormat), plugin.WithCustomFormatsOnly())
	}

	desc, err := pm.Describe(ctx, pluginName, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to describe format '%s': %w", name, err)
	}

	// prefer custom format (order of precedence)
	if format, isCustom := desc.CustomFormats[name]; isCustom {
		loc := "config"
		if filePath != "" {
			loc = filePath

		}
		return NewFormatResource(loc, format), nil
	}

	// if format is a preset
	if format, isPreset := desc.FormatPresets[name]; isPreset {
		return NewFormatResource(pluginName, format), nil
	}

	return nil, fmt.Errorf("format '%s' not found", name)
}
