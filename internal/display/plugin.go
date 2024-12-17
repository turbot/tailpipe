package display

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

type PluginResource struct {
	Name       string   `json:"name"`
	Version    string   `json:"version"`
	Sources    []string `json:"sources"`
	Partitions []string `json:"partitions"`
	Tables     []string `json:"tables"`
}

func ListPluginResources(ctx context.Context) ([]*PluginResource, error) {
	var res []*PluginResource

	pluginManager := plugin_manager.New()

	basicInfo, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	for _, p := range basicInfo {
		desc, err := pluginManager.Describe(ctx, p.Name)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
		}

		var sources []string
		for _, source := range desc.Sources {
			sources = append(sources, source.Name)
		}
		slices.Sort(sources)

		var tables []string
		for table, _ := range desc.TableSchemas {
			tables = append(tables, table)
		}
		slices.Sort(tables)

		pr := &PluginResource{
			Name:    p.Name,
			Version: p.Version.String(),
			Sources: sources,
			Tables:  tables,
		}

		pr.setPartitions()

		res = append(res, pr)
	}

	return res, nil
}

func GetPluginResource(ctx context.Context, name string) (*PluginResource, error) {

	pluginManager := plugin_manager.New()

	basicInfo, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, &name)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	if len(basicInfo) == 0 {
		return nil, fmt.Errorf("plugin %s not found", name)
	}

	p := basicInfo[0]

	desc, err := pluginManager.Describe(ctx, p.Name)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
	}

	var sources []string
	for _, source := range desc.Sources {
		sources = append(sources, source.Name)
	}
	slices.Sort(sources)

	var tables []string
	for table, _ := range desc.TableSchemas {
		tables = append(tables, table)
	}
	slices.Sort(tables)

	pr := &PluginResource{
		Name:    p.Name,
		Version: p.Version.String(),
		Sources: sources,
		Tables:  tables,
	}

	pr.setPartitions()

	return pr, nil
}

func (r *PluginResource) setPartitions() {
	for _, partition := range config.GlobalConfig.Partitions {
		if partition.Plugin.Plugin == r.Name {
			r.Partitions = append(r.Partitions, strings.TrimPrefix(partition.FullName, "partition."))
		}
	}

	slices.Sort(r.Partitions)
}

// GetShowData implements the printers.Showable interface
func (r *PluginResource) GetShowData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Version", r.Version),
		printers.NewFieldValue("Sources", r.Sources),
		printers.NewFieldValue("Tables", r.Tables),
		printers.NewFieldValue("Partitions", r.Partitions),
	)
	return res
}

// GetListData implements the printers.Listable interface
func (r *PluginResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("INSTALLED", r.Name),
		printers.NewFieldValue("VERSION", r.Version),
		printers.NewFieldValue("PARTITIONS", strings.Join(r.Partitions, ", ")),
	)
	return res
}
