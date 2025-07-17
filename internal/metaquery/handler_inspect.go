package metaquery

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/turbot/tailpipe/internal/helpers"
	"github.com/turbot/tailpipe/internal/plugin"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
)

// inspect
func inspect(ctx context.Context, input *HandlerInput) error {
	views, err := input.GetViews()
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	if len(input.args()) == 0 {
		return listViews(ctx, input, views)
	}

	viewName := input.args()[0]
	if slices.Contains(views, viewName) {
		return listViewSchema(ctx, input, viewName)
	}

	return fmt.Errorf("could not find a view named '%s'", viewName)
}

func listViews(ctx context.Context, input *HandlerInput, views []string) error {
	var rows [][]string
	rows = append(rows, []string{"Table", "Plugin"}) // Header

	for _, view := range views {
		// TODO look at using config.GetPluginForTable(ctx, view) instead of this - or perhaps add function
		// GetPluginAndVersionForTable?
		// getPluginForTable looks at plugin binaries (slower but mre reliable)
		p, _ := getPluginForTable(ctx, view)
		rows = append(rows, []string{view, p})
	}

	fmt.Println(buildTable(rows, false)) //nolint:forbidigo //UI output
	return nil
}

func listViewSchema(ctx context.Context, input *HandlerInput, viewName string) error {
	schema, err := database.GetTableViewSchema(ctx, viewName)
	if err != nil {
		return fmt.Errorf("failed to get view schema: %w", err)
	}

	var rows [][]string
	rows = append(rows, []string{"Column", "Type"}) // Header

	var cols []string
	for column := range schema {
		cols = append(cols, column)
	}

	// Sort column names alphabetically but with tp_ fields on the end
	cols = helpers.SortColumnsAlphabetically(cols)

	for _, col := range cols {
		rows = append(rows, []string{col, strings.ToLower(schema[col])})
	}

	fmt.Println(buildTable(rows, false)) //nolint:forbidigo //UI output
	return nil
}

// getPluginForTable returns the plugin name and version for a given table name.
// note - this looks at the installed plugins and their version file entry, not only the version file
func getPluginForTable(ctx context.Context, tableName string) (string, error) {
	// First check if this is a custom table
	if _, isCustom := config.GlobalConfig.CustomTables[tableName]; isCustom {
		// Custom tables use the core plugin
		corePluginName := constants.CorePluginInstallStream()
		return corePluginName, nil
	}

	prefix := strings.Split(tableName, "_")[0]

	ps, err := plugin.GetInstalledPlugins(ctx, config.GlobalConfig.PluginVersions)
	if err != nil {
		return "", fmt.Errorf("failed to get installed plugins: %w", err)
	}

	for k, v := range ps {
		pluginShortName := strings.Split(k, "/")[1]
		if strings.HasPrefix(pluginShortName, prefix) {
			return fmt.Sprintf("%s@%s", pluginShortName, v.String()), nil
		}
	}

	return "", nil
}
