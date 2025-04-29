package display

import (
	"context"
	"fmt"
	"github.com/iancoleman/strcase"
	"github.com/turbot/tailpipe-plugin-sdk/types"

	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
)

type SourceResource struct {
	Name        string                             `json:"name"`
	Description string                             `json:"description,omitempty"`
	Plugin      string                             `json:"plugin,omitempty"`
	Properties  map[string]*types.PropertyMetadata `json:"properties,omitempty"`
}

// GetShowData implements the printers.Showable interface
func (r *SourceResource) GetShowData() *printers.RowData {
	var allProperties = []printers.FieldValue{
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Plugin", r.Plugin),
		printers.NewFieldValue("Description", r.Description),
	}
	if len(r.Properties) > 0 {
		args := map[string]string{}

		for k, v := range r.Properties {
			propertyString := v.Type
			if v.Description != "" {
				propertyString += ": " + v.Description
			}
			if v.Required {
				propertyString += " (required)"
			}
			// convert field-name to camel case for pretty display
			name := strcase.ToCamel(k)
			args[name] = propertyString
		}
		allProperties = append(allProperties, printers.NewFieldValue("Properties", args))
	}
	res := printers.NewRowData(allProperties...)
	return res
}

// GetListData implements the printers.Listable interface
func (r *SourceResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("PLUGIN", r.Plugin),
		printers.NewFieldValue("DESCRIPTION", r.Description),
	)
	return res
}

func ListSourceResources(ctx context.Context) ([]*SourceResource, error) {
	var res []*SourceResource

	pluginManager := plugin.NewPluginManager()
	defer pluginManager.Close()

	plugins, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	for _, p := range plugins {
		desc, err := pluginManager.Describe(ctx, p.Name)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
		}

		for _, source := range desc.Sources {
			res = append(res, &SourceResource{
				Name:        source.Name,
				Description: source.Description,
				Plugin:      p.Name,
			})
		}
	}

	return res, nil
}

func GetSourceResource(ctx context.Context, sourceName string) (*SourceResource, error) {

	// get the plugin which provides the format (note  config.GetPluginForFormatByName is smart enough
	// to check whether this format is a preset and if so, return the plugin which provides the preset)
	pluginName := config.GetPluginForSourceType(sourceName, config.GlobalConfig.PluginVersions)

	// describe plugin to get format info, passing the custom formats list in case this is custom
	// (we will have determined this above)
	pm := plugin.NewPluginManager()
	defer pm.Close()

	desc, err := pm.Describe(ctx, pluginName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe source '%s': %w", sourceName, err)
	}

	source, ok := desc.Sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("source '%s' not found", sourceName)
	}
	return &SourceResource{
		Name:        source.Name,
		Description: source.Description,
		Properties:  source.Properties,
		Plugin:      pluginName,
	}, nil

}
