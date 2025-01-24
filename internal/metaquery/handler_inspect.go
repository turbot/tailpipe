package metaquery

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/plugin"
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

	return nil
}

func listViews(ctx context.Context, input *HandlerInput, views []string) error {
	var rows [][]string
	rows = append(rows, []string{"Table", "Plugin"}) // Header

	for _, view := range views {
		p, _ := getPluginForTable(ctx, view)
		rows = append(rows, []string{view, p})
	}

	fmt.Println(buildTable(rows, true)) //nolint:forbidigo //UI output
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
	sort.Strings(cols)

	for _, col := range cols {
		rows = append(rows, []string{col, schema[col]})
	}

	fmt.Println(buildTable(rows, true)) //nolint:forbidigo //UI output
	return nil
}

func getPluginForTable(ctx context.Context, tableName string) (string, error) {
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
