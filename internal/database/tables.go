package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

// GetTables returns the list of tables in the DuckLake metadata catalog
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

// GetTableSchema returns the schema of the specified table as a map of column names to their types
func GetTableSchema(ctx context.Context, tableName string, db *DuckDb) (map[string]string, error) {
	query := fmt.Sprintf(`select c.column_name, c.column_type
from %s.ducklake_table t
join %s.ducklake_column c
  on t.table_id = c.table_id
where t.table_name = ?
order by c.column_name;`, constants.DuckLakeMetadataCatalog, constants.DuckLakeMetadataCatalog)

	rows, err := db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get view schema for %s: %w", tableName, err)
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

// GetLegacyTableViews retrieves the names of all table views in the legacy database(tailpipe.db) file
func GetLegacyTableViews(ctx context.Context, db *DuckDb) ([]string, error) {
	query := "select table_name from information_schema.tables where table_type='VIEW';"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table views: %w", err)
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

// GetLegacyTableViewSchema retrieves the schema of a table view in the legacy database(tailpipe.db) file
func GetLegacyTableViewSchema(ctx context.Context, viewName string, db *DuckDb) (*schema.TableSchema, error) {
	query := `
		select column_name, data_type 
		from information_schema.columns 
		where table_name = ? ORDER BY columns.column_name;
	`
	rows, err := db.QueryContext(ctx, query, viewName)
	if err != nil {
		return nil, fmt.Errorf("failed to get view schema for %s: %w", viewName, err)
	}
	defer rows.Close()

	ts := &schema.TableSchema{
		Name:    viewName,
		Columns: []*schema.ColumnSchema{},
	}
	for rows.Next() {
		// here each row is a column, so we need to populate the TableSchema.Columns, particularly the
		// ColumnName, Type and StructFields
		var columnName, columnType string
		err = rows.Scan(&columnName, &columnType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column schema: %w", err)
		}

		// NOTE: legacy tailpipe views may include `rowid` which we must exclude from the schema as this is a DuckDb system column
		// that is automatically added to every table
		if columnName == "rowid" {
			continue
		}
		col := buildColumnSchema(columnName, columnType)
		ts.Columns = append(ts.Columns, col)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over view schema rows: %w", err)
	}

	return ts, nil
}

// buildColumnSchema constructs a ColumnSchema from a DuckDB data type string.
// It handles primitive types as well as struct and struct[] recursively, populating StructFields.
func buildColumnSchema(columnName string, duckdbType string) *schema.ColumnSchema {
	t := strings.TrimSpace(duckdbType)
	lower := strings.ToLower(t)

	// Helper to set basic column properties
	newCol := func(name, typ string, children []*schema.ColumnSchema) *schema.ColumnSchema {
		return &schema.ColumnSchema{
			ColumnName:   name,
			SourceName:   name,
			Type:         typ,
			StructFields: children,
		}
	}

	// Detect struct or struct[]
	if strings.HasPrefix(lower, "struct(") || strings.HasPrefix(lower, "struct ") {
		isArray := false
		// Handle optional [] suffix indicating array of struct
		if strings.HasSuffix(lower, ")[]") {
			isArray = true
		}
		// Extract inner content between the first '(' and the matching ')'
		start := strings.Index(t, "(")
		end := strings.LastIndex(t, ")")
		inner := ""
		if start >= 0 && end > start {
			inner = strings.TrimSpace(t[start+1 : end])
		}

		fields := parseStructFields(inner)
		typ := "struct"
		if isArray {
			typ = "struct[]"
		}
		return newCol(columnName, typ, fields)
	}

	// Primitive or other complex types - just set as-is
	return newCol(columnName, lower, nil)
}

// parseStructFields parses the content inside a DuckDB struct(...) definition into ColumnSchemas.
// It supports nested struct/struct[] types by recursively building ColumnSchemas for child fields.
func parseStructFields(inner string) []*schema.ColumnSchema {
	// Split by top-level commas (not within nested parentheses)
	parts := splitTopLevel(inner, ',')
	var fields []*schema.ColumnSchema
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// parse field name (optionally quoted) and type
		name, typ := parseFieldNameAndType(p)
		if name == "" || typ == "" {
			continue
		}
		col := buildColumnSchema(name, typ)
		fields = append(fields, col)
	}
	return fields
}

// parseFieldNameAndType parses a single struct field spec of the form:
//
//	name type
//	"name with spaces" type
//
// where type may itself be struct(...)[]. Returns name and the raw type string.
func parseFieldNameAndType(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	if s[0] == '"' {
		// quoted name
		// find closing quote
		i := 1
		for i < len(s) && s[i] != '"' {
			i++
		}
		if i >= len(s) {
			return "", ""
		}
		name := s[1:i]
		rest := strings.TrimSpace(s[i+1:])
		// rest should start with the type
		return name, rest
	}
	// unquoted name up to first space
	idx := strings.IndexFunc(s, func(r rune) bool { return r == ' ' || r == '\t' })
	if idx == -1 {
		// no type specified
		return "", ""
	}
	name := strings.TrimSpace(s[:idx])
	typ := strings.TrimSpace(s[idx+1:])
	return name, typ
}

// splitTopLevel splits s by sep, ignoring separators enclosed in parentheses.
func splitTopLevel(s string, sep rune) []string {
	var res []string
	level := 0
	start := 0
	for i, r := range s {
		switch r {
		case '(':
			level++
		case ')':
			if level > 0 {
				level--
			}
		}
		if r == sep && level == 0 {
			res = append(res, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	// add last segment
	if start <= len(s) {
		res = append(res, strings.TrimSpace(s[start:]))
	}
	return res
}
