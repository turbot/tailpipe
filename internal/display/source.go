package display

import (
	"context"
	"fmt"

	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

type SourceResource struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// GetShowData implements the printers.Showable interface
func (r *SourceResource) GetShowData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
	)
	return res
}

// GetListData implements the printers.Listable interface
func (r *SourceResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("DESCRIPTION", r.Description),
	)
	return res
}

func ListSourceResources(ctx context.Context) ([]*SourceResource, error) {
	var res []*SourceResource

	pluginManager := plugin_manager.New()
	defer pluginManager.Close()

	plugins, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	// TODO: #refactor `seen` map is only needed as each plugin currently implements file_system source due to being in SDK
	var seen = make(map[string]bool)

	for _, p := range plugins {
		desc, err := pluginManager.Describe(ctx, p.Name)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
		}

		for _, source := range desc.Sources {
			if seen[source.Name] {
				continue
			}
			res = append(res, &SourceResource{
				Name:        source.Name,
				Description: source.Description,
			})
			seen[source.Name] = true
		}
	}

	return res, nil
}

func GetSourceResource(ctx context.Context, sourceName string) (*SourceResource, error) {
	// TODO: #refactor simplify by obtaining correct plugin and then extracting it's source
	allSources, err := ListSourceResources(ctx)
	if err != nil {
		return nil, err
	}

	for _, source := range allSources {
		if source.Name == sourceName {
			return source, nil
		}
	}

	return nil, fmt.Errorf("source %s not found", sourceName)
}
