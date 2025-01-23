package display

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/pipe-fittings/sanitize"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/plugin"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

type TableResource struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Plugin      string                 `json:"plugin"`
	Partitions  []string               `json:"partitions,omitempty"`
	Columns     []*schema.ColumnSchema `json:"columns"`
	Local       TableResourceFiles     `json:"local,omitempty"`
}

type TableResourceFiles struct {
	FileMetadata
	RowCount int64 `json:"row_count,omitempty"`
}

// GetShowData implements the printers.Showable interface
func (r *TableResource) GetShowData() *printers.RowData {

	statusString := fmt.Sprintf("\n  %d local file(s)\n  %s local", r.Local.FileCount, humanizeBytes(r.Local.FileSize))

	res := printers.NewRowData(
		printers.NewFieldValue("Name", r.Name),
		printers.NewFieldValue("Description", r.Description),
		printers.NewFieldValue("Columns", r.Columns, printers.WithRenderValueFunc(r.getColumnsRenderFunc())),
		printers.NewFieldValue("Status", statusString),
		printers.NewFieldValue("Partitions", r.Partitions),
	)
	return res
}

// GetListData implements the printers.Listable interface
func (r *TableResource) GetListData() *printers.RowData {
	res := printers.NewRowData(
		printers.NewFieldValue("NAME", r.Name),
		printers.NewFieldValue("PLUGIN", r.Plugin),
		printers.NewFieldValue("LOCAL SIZE", humanizeBytes(r.Local.FileSize)),
		printers.NewFieldValue("FILES", humanize.Comma(r.Local.FileCount)),
		printers.NewFieldValue("ROWS", humanize.Comma(r.Local.RowCount)),
		printers.NewFieldValue("DESCRIPTION", r.Description),
	)
	return res
}

func ListTableResources(ctx context.Context) ([]*TableResource, error) {
	var res []*TableResource
	pluginManager := plugin_manager.New()
	defer pluginManager.Close()

	plugins, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	// Get Plugin Defined Tables
	for _, p := range plugins {
		desc, err := pluginManager.Describe(ctx, p.Name)
		if err != nil {
			return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
		}

		for t, s := range desc.TableSchemas {
			table := &TableResource{
				Name:        t,
				Description: s.Description,
				Plugin:      p.Name,
				Columns:     s.Columns,
			}

			table.setPartitions()
			err = table.setFileInformation()
			if err != nil {
				return nil, fmt.Errorf("unable to obtain file information: %w", err)
			}

			res = append(res, table)
		}
	}

	// TODO: get config defined tables

	return res, nil
}

func GetTableResource(ctx context.Context, tableName string) (*TableResource, error) {
	pluginManager := plugin_manager.New()
	defer pluginManager.Close()
	pluginName := strings.Split(tableName, "_")[0]

	plugins, err := plugin.List(ctx, config.GlobalConfig.PluginVersions, &pluginName)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin list: %w", err)
	}

	if len(plugins) == 0 {
		return nil, fmt.Errorf("plugin %s not found", pluginName)
	}

	p := plugins[0]

	desc, err := pluginManager.Describe(ctx, p.Name)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
	}

	if tableSchema, ok := desc.TableSchemas[tableName]; ok {
		table := &TableResource{
			Name:        tableName,
			Description: "", // TODO: obtain table description, currently not available in TableSchemas
			Plugin:      p.Name,
			Columns:     tableSchema.Columns,
		}

		table.setPartitions()
		err = table.setFileInformation()
		if err != nil {
			return nil, fmt.Errorf("unable to obtain file information: %w", err)
		}

		return table, nil
	} else {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
}

func (r *TableResource) setPartitions() {
	for _, partition := range config.GlobalConfig.Partitions {
		if partition.TableName == r.Name {
			r.Partitions = append(r.Partitions, partition.ShortName)
		}
	}

	slices.Sort(r.Partitions)
}

func (r *TableResource) setFileInformation() error {
	metadata, err := getFileMetadata(path.Join(config.GlobalWorkspaceProfile.GetDataDir(), fmt.Sprintf("%s=%s", constants.TpTable, r.Name)))
	if err != nil {
		return fmt.Errorf("unable to obtain file metadata: %w", err)
	}

	r.Local.FileMetadata = metadata

	if metadata.FileCount > 0 {
		var rc int64
		rc, err = database.GetRowCount(context.Background(), r.Name, nil)
		if err != nil {
			return fmt.Errorf("unable to obtain row count: %w", err)
		}
		r.Local.RowCount = rc
	}

	return nil
}

func (r *TableResource) getColumnsRenderFunc() printers.RenderFunc {
	return func(opts sanitize.RenderOptions) string {
		var lines []string
		lines = append(lines, "") // blank line before column details

		for _, c := range r.Columns {
			line := fmt.Sprintf("  %s: %s", c.ColumnName, c.Type)
			lines = append(lines, line)
		}

		return strings.Join(lines, "\n")
	}
}
