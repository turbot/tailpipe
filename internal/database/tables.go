package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/v2/constants"
)

func GetTables(ctx context.Context, db *DuckDb) ([]string, error) {

	query := fmt.Sprintf("select table_name from %s.ducklake_table", constants.DuckLakeMetadataCatalog)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	defer rows.Close()

	var tableViews []string
	for rows.Next() {
		var tableView string
		err = rows.Scan(&tableView)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table view: %w", err)
		}
		tableViews = append(tableViews, tableView)
	}
	return tableViews, nil
}

func GetTableSchema(ctx context.Context, viewName string, db *DuckDb) (map[string]string, error) {

	query := fmt.Sprintf(`select c.column_name, c.column_type
from %s.ducklake_table t
join %s.ducklake_column c
  on t.table_id = c.table_id
where t.table_name = ?
order by c.column_name;`, constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog)

	rows, err := db.QueryContext(ctx, query, viewName)
	if err != nil {
		return nil, fmt.Errorf("failed to get view schema for %s: %w", viewName, err)
	}
	defer rows.Close()

	schema := make(map[string]string)
	for rows.Next() {
		var columnName, columnType string
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column schema: %w", err)
		}
		if strings.HasPrefix(columnType, "struct") {
			columnType = "struct"
		}
		schema[columnName] = columnType
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over view schema rows: %w", err)
	}

	return schema, nil
}
