package metaquery

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/turbot/tailpipe/internal/config"
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
		p := config.GlobalConfig.GetPluginForTable(view)
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
	sort.Strings(cols)

	for _, col := range cols {
		rows = append(rows, []string{col, strings.ToLower(schema[col])})
	}

	fmt.Println(buildTable(rows, false)) //nolint:forbidigo //UI output
	return nil
}
