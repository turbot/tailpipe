package database

import (
	"context"
	"fmt"
	"strings"

	pconstants "github.com/turbot/pipe-fittings/v2/constants"
)

// GetCreateViewsSql returns the SQL commands to create views for all tables in the DuckLake catalog,
//
//	applying the specified filters.
func GetCreateViewsSql(ctx context.Context, db *DuckDb, filters ...string) ([]SqlCommand, error) {
	// get list of tables
	tables, err := GetTables(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to get db tables: %w", err)
	}

	// Step 3: Build the where clause
	filterString := ""
	if len(filters) > 0 {
		filterString = fmt.Sprintf(" where %s", strings.Join(filters, " and "))
	}

	results := make([]SqlCommand, 0, len(tables))
	for _, table := range tables {
		description := fmt.Sprintf("Create View for table %s", table)
		command := fmt.Sprintf("create or replace view %s as select * from %s.%s%s", table, pconstants.DuckLakeCatalog, table, filterString)
		results = append(results, SqlCommand{Description: description, Command: command})
	}
	return results, nil
}
