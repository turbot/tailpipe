package display

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/dustin/go-humanize"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/printers"
	"github.com/turbot/pipe-fittings/v2/sanitize"
	sdkconstants "github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/plugin"
)

// TableResource represents a table resource for display purposes (list/show)
type TableResource struct {
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Plugin      string                `json:"plugin"`
	Partitions  []string              `json:"partitions,omitempty"`
	Columns     []TableColumnResource `json:"columns"`
	Local       TableResourceFiles    `json:"local,omitempty"`
}

// tableResourceFromConfigTable creates a TableResource (display item) from a config.Table (custom table)
func tableResourceFromConfigTable(tableName string, configTable *config.Table) (*TableResource, error) {
	cols := make([]TableColumnResource, len(configTable.Columns))
	for i, c := range configTable.Columns {
		cols[i] = TableColumnResource{
			ColumnName: c.Name,
			Type:       types.SafeString(c.Type),
		}
	}

	table := &TableResource{
		Name:        tableName,
		Description: types.SafeString(configTable.Description),
		Plugin:      constants.CorePluginName,
		Columns:     cols,
	}

	table.setPartitions()
	err := table.setFileInformation()
	if err != nil {
		return nil, fmt.Errorf("failed to set file information for table '%s': %w", tableName, err)
	}

	return table, nil
}

// tableResourceFromSchemaTable creates a TableResource (display item) from a schema.TableSchema (defined table)
func tableResourceFromSchemaTable(tableName string, pluginName string, schemaTable *schema.TableSchema) (*TableResource, error) {
	cols := make([]TableColumnResource, len(schemaTable.Columns))
	for i, c := range schemaTable.Columns {
		cols[i] = TableColumnResource{
			ColumnName: c.ColumnName,
			Type:       c.Type,
		}
	}

	table := &TableResource{
		Name:        tableName,
		Description: schemaTable.Description,
		Plugin:      pluginName,
		Columns:     cols,
	}

	table.setPartitions()
	err := table.setFileInformation()
	if err != nil {
		return nil, fmt.Errorf("failed to set file information for table '%s': %w", tableName, err)
	}

	return table, nil
}

// TableColumnResource represents a table column for display purposes
type TableColumnResource struct {
	ColumnName string `json:"column_name"`
	Type       string `json:"type"`
}

// TableResourceFiles represents the file information and a row count for a table resource
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
	tables := make(map[string]*TableResource)

	// get plugin defined tables first
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

		for t, s := range desc.Schemas {
			table, err := tableResourceFromSchemaTable(t, p.Name, s)
			if err != nil {
				return nil, err
			}

			tables[t] = table
		}
	}

	// custom tables - these take precedence over plugin defined tables, so overwrite any duplicates in map
	for tableName, tableDef := range config.GlobalConfig.CustomTables {
		table, err := tableResourceFromConfigTable(tableName, tableDef)
		if err != nil {
			return nil, err
		}

		tables[tableName] = table
	}

	// build output list from map
	for _, table := range tables {
		res = append(res, table)
	}

	return res, nil
}

func GetTableResource(ctx context.Context, tableName string) (*TableResource, error) {
	// custom table takes precedence over plugin defined table, check there first
	if customTable, ok := config.GlobalConfig.CustomTables[tableName]; ok {
		table, err := tableResourceFromConfigTable(tableName, customTable)
		return table, err
	}

	// obtain table from plugin describe call
	pluginManager := plugin.NewPluginManager()
	defer pluginManager.Close()

	pluginName := config.GetPluginForTable(tableName, config.GlobalConfig.PluginVersions)
	// if this is a custom table, we need to use the core plugin
	// NOTE: we cannot do this inside GetPluginForTable as that funciton may be called before the config is fully populated
	if _, isCustom := config.GlobalConfig.CustomTables[tableName]; isCustom {
		pluginName = constants.CorePluginName
	}

	desc, err := pluginManager.Describe(ctx, pluginName)
	if err != nil {
		return nil, fmt.Errorf("unable to obtain plugin details: %w", err)
	}

	if tableSchema, ok := desc.Schemas[tableName]; ok {
		return tableResourceFromSchemaTable(tableName, pluginName, tableSchema)
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
	metadata, err := getFileMetadata(path.Join(config.GlobalWorkspaceProfile.GetDataDir(), fmt.Sprintf("%s=%s", sdkconstants.TpTable, r.Name)))
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

		cols := r.Columns
		// TODO: #graza we utilize similar behaviour in the view creation but only on string, can we combine these into a single func?
		tpPrefix := "tp_"
		slices.SortFunc(cols, func(a, b TableColumnResource) int {
			isPrefixedA, isPrefixedB := strings.HasPrefix(a.ColumnName, tpPrefix), strings.HasPrefix(b.ColumnName, tpPrefix)
			switch {
			case isPrefixedA && !isPrefixedB:
				return 1 // a > b
			case !isPrefixedA && isPrefixedB:
				return -1 // a < b
			default:
				return strings.Compare(a.ColumnName, b.ColumnName) // standard alphabetical sort
			}
		})

		for _, c := range r.Columns {
			line := fmt.Sprintf("  %s: %s", c.ColumnName, c.Type)
			lines = append(lines, line)
		}

		return strings.Join(lines, "\n")
	}
}
