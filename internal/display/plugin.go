package display

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/plugin"
)

type PluginListDetails struct {
	Name       string   `json:"name"`
	Version    string   `json:"version"`
	Partitions []string `json:"partitions"`
}

// GetListData implements the printers.Listable interface
func (r *PluginListDetails) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("INSTALLED", r.Name),
		printers.NewFieldValue("VERSION", r.Version),
		printers.NewFieldValue("PARTITIONS", strings.Join(r.Partitions, ", ")),
	)
	return res
}

func (r *PluginListDetails) setPartitions() {
	for _, partition := range config.GlobalConfig.Partitions {
		if partition.Plugin.Plugin == r.Name || (r.Version == "local" && lastSegment(r.Name) == partition.Plugin.Alias) {
			r.Partitions = append(r.Partitions, strings.TrimPrefix(partition.FullName, "partition."))
		}
	}

	slices.Sort(r.Partitions)
}

func lastSegment(s string) string {
	p := strings.Split(strings.Trim(s, "/"), "/")
	return p[len(p)-1]
}

func ListPlugins(ctx context.Context) ([]*PluginListDetails, error) {
	var res []*PluginListDetails

	basicInfo, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	for _, p := range basicInfo {
		d := &PluginListDetails{
			Name:    p.Name,
			Version: p.Version.String(),
		}
		d.setPartitions()

		res = append(res, d)
	}

	return res, nil
}

type PluginResource struct {
	Name          string   `json:"name"`
	Version       string   `json:"version"`
	Sources       []string `json:"sources"`
	Partitions    []string `json:"partitions"`
	Tables        []string `json:"tables"`
	FormatTypes   []string `json:"format_types"`
	FormatPresets []string `json:"format_presets"`
}

func GetPluginResource(ctx context.Context, name string) (*PluginResource, error) {
	pluginManager := plugin.NewPluginManager()
	defer pluginManager.Close()

	desc, err := pluginManager.Describe(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
	}

	var version string
	if pv, ok := config.GlobalConfig.PluginVersions[desc.PluginName]; ok {
		version = pv.Version
	}

	var sources []string
	for _, source := range desc.Sources {
		sources = append(sources, source.Name)
	}
	slices.Sort(sources)

	var tables []string
	for table := range desc.Schemas {
		tables = append(tables, table)
	}
	slices.Sort(tables)

	var formatTypes []string
	formatTypes = append(formatTypes, desc.FormatTypes...)
	slices.Sort(formatTypes)

	var formatPresets []string
	for formatPreset := range desc.FormatPresets {
		formatPresets = append(formatPresets, formatPreset)
	}
	slices.Sort(formatPresets)

	pr := &PluginResource{
		Name:          desc.PluginName,
		Version:       version,
		Sources:       sources,
		Tables:        tables,
		FormatTypes:   formatTypes,
		FormatPresets: formatPresets,
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
		printers.NewFieldValue("Format Types", r.FormatTypes),
		printers.NewFieldValue("Format Presets", r.FormatPresets),
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
