package metaquery

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/turbot/pipe-fittings/v2/ociinstaller"
	pluginsdk "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe-plugin-sdk/helpers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/plugin"
)

// inspect
func inspect(ctx context.Context, input *HandlerInput) error {
	tables, err := input.GetTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to get tables: %w", err)
	}

	if len(input.args()) == 0 {
		return listTables(ctx, input, tables)
	}

	tableName := input.args()[0]
	if slices.Contains(tables, tableName) {
		return getTableSchema(ctx, input, tableName)
	}

	return fmt.Errorf("could not find a view named '%s'", tableName)
}

func listTables(ctx context.Context, input *HandlerInput, views []string) error {
	var rows [][]string
	rows = append(rows, []string{"Table", "Plugin"}) // Header

	// installed plugins (with versions) are the same for every table - read them once
	installed, err := plugin.GetInstalledPlugins(ctx, config.GlobalConfig.PluginVersions)
	if err != nil {
		return fmt.Errorf("failed to get installed plugins: %w", err)
	}

	for _, view := range views {
		p := getPluginForTable(view, config.GlobalConfig.CustomTables, config.GlobalConfig.PluginVersions, installed)
		rows = append(rows, []string{view, p})
	}

	fmt.Println(buildTable(rows, false)) //nolint:forbidigo //UI output
	return nil
}

func getTableSchema(ctx context.Context, input *HandlerInput, tableName string) error {
	schema, err := database.GetTableSchema(ctx, tableName, input.Db)
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
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

// getPluginForTable returns the plugin name and version ("<name>@<version>") for a given table name,
// or an empty string if the plugin is not installed.
// The plugin is found in the same way as `tailpipe table show`: custom tables use the core plugin,
// and any other table belongs to the plugin whose recorded metadata lists it
// (see config.GetPluginForTable), so the table name need not start with the plugin name.
// installed is keyed by "<org>/<name>", as returned by plugin.GetInstalledPlugins
func getPluginForTable(tableName string, customTables map[string]*config.Table, versionMap map[string]*versionfile.InstalledVersion, installed map[string]*pluginsdk.PluginVersionString) string {
	var fullName string
	if _, isCustom := customTables[tableName]; isCustom {
		// Custom tables use the core plugin
		fullName = constants.CorePluginInstallStream()
	} else {
		fullName = config.GetPluginForTable(tableName, versionMap)
	}

	org, name, _ := ociinstaller.NewImageRef(fullName).GetOrgNameAndStream()
	version, ok := installed[fmt.Sprintf("%s/%s", org, name)]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%s@%s", name, version.String())
}
