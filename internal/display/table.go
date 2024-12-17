package display

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/dustin/go-humanize"

	"github.com/turbot/pipe-fittings/printers"
	"github.com/turbot/pipe-fittings/sanitize"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/plugin"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

type TableResource struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description,omitempty"`
	Plugin      string                 `json:"plugin"`
	Partitions  []string               `json:"partitions"`
	Columns     []*schema.ColumnSchema `json:"columns"`
	Local       TableResourceFiles     `json:"local,omitempty"`
	//Remote      TableResourceFiles     `json:"remote,omitempty"`
}

type TableResourceFiles struct {
	FileSize  int64 `json:"file_size"`
	FileCount int64 `json:"file_count"`
	RowCount  int64 `json:"row_count,omitempty"`
}

// GetShowData implements the printers.Showable interface
func (r *TableResource) GetShowData() *printers.RowData {

	statusString := fmt.Sprintf("\n  %d local file(s)\n  %s local", r.Local.FileCount, humanize.Bytes(uint64(r.Local.FileSize)))

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
		printers.NewFieldValue("LOCAL SIZE", humanize.Bytes(uint64(r.Local.FileSize))),
		printers.NewFieldValue("FILES", humanize.Comma(r.Local.FileCount)),
		printers.NewFieldValue("ROWS", humanize.Comma(r.Local.RowCount)),
	)
	return res
}

func ListTableResources(ctx context.Context) ([]*TableResource, error) {
	var res []*TableResource
	pluginManager := plugin_manager.New()

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
				Description: "", // TODO: obtain table description, currently not available in TableSchemas
				Plugin:      p.Name,
				Columns:     s.Columns,
			}

			table.setPartitions()
			err = table.setFileInformation()
			if err != nil {
				return nil, fmt.Errorf("unable to obtain file information: %w", err)
			}

			if table.Local.FileCount > 0 {
				rc, err := database.GetRowCount(ctx, t)
				if err != nil {
					return nil, fmt.Errorf("unable to obtain row count: %w", err)
				}
				table.Local.RowCount = rc
			}

			res = append(res, table)
		}
	}

	// TODO: get config defined tables

	return res, nil
}

func GetTableResource(ctx context.Context, tableName string) (*TableResource, error) {
	pluginManager := plugin_manager.New()
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

		if table.Local.FileCount > 0 {
			rc, err := database.GetRowCount(ctx, tableName)
			if err != nil {
				return nil, fmt.Errorf("unable to obtain row count: %w", err)
			}
			table.Local.RowCount = rc
		}

		return table, nil
	} else {
		return nil, fmt.Errorf("table %s not found", tableName)
	}
}

func (r *TableResource) setPartitions() {
	for _, partition := range config.GlobalConfig.Partitions {
		if partition.Table == r.Name {
			r.Partitions = append(r.Partitions, partition.ShortName)
		}
	}

	slices.Sort(r.Partitions)
}

func (r *TableResource) setFileInformation() error {
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	tableDir := path.Join(dataDir, fmt.Sprintf("tp_table=%s", r.Name))

	// if tableDir doesn't exist - nothing collected so short-circuit
	if _, err := os.Stat(tableDir); os.IsNotExist(err) {
		return nil
	}

	// Get File Information
	err := filepath.Walk(tableDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		r.Local.FileCount++
		r.Local.FileSize += info.Size()

		return nil
	})

	return err
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
