package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turbot/tailpipe/internal/database"
)

func TestGetColumnDefsForQuery(t *testing.T) {
	// Create a temporary DuckDB instance for testing
	db, err := database.NewDuckDb(database.WithTempDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	// Create test tables with sample data
	setupTestTables(t, db)

	tests := []struct {
		name         string
		query        string
		expectedCols []string
		expectError  bool
	}{
		{
			name:         "simple select",
			query:        "SELECT id, name, value FROM test_table",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "select with aliases",
			query:        "SELECT id AS user_id, name AS user_name, value AS score FROM test_table",
			expectedCols: []string{"user_id", "user_name", "score"},
			expectError:  false,
		},
		{
			name:         "select with functions",
			query:        "SELECT COUNT(*), AVG(value), MAX(name) FROM test_table",
			expectedCols: []string{"count_star()", "avg(\"value\")", "max(\"name\")"},
			expectError:  false,
		},
		{
			name:         "inner join",
			query:        "SELECT t1.id, t1.name, t2.category FROM test_table t1 INNER JOIN category_table t2 ON t1.id = t2.id",
			expectedCols: []string{"id", "name", "category"},
			expectError:  false,
		},
		{
			name:         "left join",
			query:        "SELECT t1.id, t1.name, t2.category FROM test_table t1 LEFT JOIN category_table t2 ON t1.id = t2.id",
			expectedCols: []string{"id", "name", "category"},
			expectError:  false,
		},
		{
			name:         "right join",
			query:        "SELECT t1.id, t1.name, t2.category FROM test_table t1 RIGHT JOIN category_table t2 ON t1.id = t2.id",
			expectedCols: []string{"id", "name", "category"},
			expectError:  false,
		},
		{
			name:         "full outer join",
			query:        "SELECT t1.id, t1.name, t2.category FROM test_table t1 FULL OUTER JOIN category_table t2 ON t1.id = t2.id",
			expectedCols: []string{"id", "name", "category"},
			expectError:  false,
		},
		{
			name:         "cross join",
			query:        "SELECT t1.id, t1.name, t2.category FROM test_table t1 CROSS JOIN category_table t2",
			expectedCols: []string{"id", "name", "category"},
			expectError:  false,
		},
		{
			name:         "group by",
			query:        "SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM test_table t1 JOIN category_table t2 ON t1.id = t2.id GROUP BY category",
			expectedCols: []string{"category", "count", "avg_value"},
			expectError:  false,
		},
		{
			name:         "group by with having",
			query:        "SELECT category, COUNT(*) as count FROM test_table t1 JOIN category_table t2 ON t1.id = t2.id GROUP BY category HAVING COUNT(*) > 1",
			expectedCols: []string{"category", "count"},
			expectError:  false,
		},
		{
			name:         "order by",
			query:        "SELECT id, name, value FROM test_table ORDER BY value DESC",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "limit and offset",
			query:        "SELECT id, name, value FROM test_table ORDER BY id LIMIT 5 OFFSET 2",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "subquery in select",
			query:        "SELECT id, name, (SELECT AVG(value) FROM test_table) as avg_all FROM test_table",
			expectedCols: []string{"id", "name", "avg_all"},
			expectError:  false,
		},
		{
			name:         "subquery in from",
			query:        "SELECT * FROM (SELECT id, name, value FROM test_table WHERE value > 5) as sub",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "subquery in where",
			query:        "SELECT id, name, value FROM test_table WHERE value > (SELECT AVG(value) FROM test_table)",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "union",
			query:        "SELECT id, name FROM test_table UNION SELECT id, category as name FROM category_table",
			expectedCols: []string{"id", "name"},
			expectError:  false,
		},
		{
			name:         "union all",
			query:        "SELECT id, name FROM test_table UNION ALL SELECT id, category as name FROM category_table",
			expectedCols: []string{"id", "name"},
			expectError:  false,
		},
		{
			name:         "case statement",
			query:        "SELECT id, name, CASE WHEN value > 5 THEN 'high' WHEN value > 2 THEN 'medium' ELSE 'low' END as level FROM test_table",
			expectedCols: []string{"id", "name", "level"},
			expectError:  false,
		},
		{
			name:         "window function",
			query:        "SELECT id, name, value, ROW_NUMBER() OVER (ORDER BY value DESC) as rank FROM test_table",
			expectedCols: []string{"id", "name", "value", "rank"},
			expectError:  false,
		},
		{
			name:         "window function with partition",
			query:        "SELECT id, name, value, ROW_NUMBER() OVER (PARTITION BY name ORDER BY value DESC) as rank FROM test_table",
			expectedCols: []string{"id", "name", "value", "rank"},
			expectError:  false,
		},
		{
			name:         "aggregate with window function",
			query:        "SELECT id, name, value, AVG(value) OVER (PARTITION BY name) as avg_by_name FROM test_table",
			expectedCols: []string{"id", "name", "value", "avg_by_name"},
			expectError:  false,
		},
		{
			name:         "cte (common table expression)",
			query:        "WITH cte AS (SELECT id, name, value FROM test_table WHERE value > 3) SELECT * FROM cte",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "multiple ctes",
			query:        "WITH cte1 AS (SELECT id, name FROM test_table), cte2 AS (SELECT id, category FROM category_table) SELECT cte1.name, cte2.category FROM cte1 JOIN cte2 ON cte1.id = cte2.id",
			expectedCols: []string{"name", "category"},
			expectError:  false,
		},
		{
			name:         "exists subquery",
			query:        "SELECT id, name FROM test_table WHERE EXISTS (SELECT 1 FROM category_table WHERE category_table.id = test_table.id)",
			expectedCols: []string{"id", "name"},
			expectError:  false,
		},
		{
			name:         "in subquery",
			query:        "SELECT id, name FROM test_table WHERE id IN (SELECT id FROM category_table WHERE category = 'A')",
			expectedCols: []string{"id", "name"},
			expectError:  false,
		},
		{
			name:         "complex nested query",
			query:        "SELECT t1.id, t1.name, t2.category, (SELECT COUNT(*) FROM test_table WHERE value > t1.value) as higher_count FROM test_table t1 LEFT JOIN category_table t2 ON t1.id = t2.id WHERE t1.value > (SELECT AVG(value) FROM test_table) ORDER BY t1.value DESC LIMIT 10",
			expectedCols: []string{"id", "name", "category", "higher_count"},
			expectError:  false,
		},
		{
			name:         "query with semicolon",
			query:        "SELECT id, name, value FROM test_table;",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "query with extra whitespace",
			query:        "   SELECT   id,   name,   value   FROM   test_table   ",
			expectedCols: []string{"id", "name", "value"},
			expectError:  false,
		},
		{
			name:         "invalid query",
			query:        "SELECT * FROM non_existent_table",
			expectedCols: nil,
			expectError:  true,
		},
		{
			name:         "syntax error",
			query:        "SELECT id, name FROM test_table WHERE",
			expectedCols: nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colDefs, err := GetColumnDefsForQuery(tt.query, db)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, colDefs)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, colDefs)
				assert.Len(t, colDefs, len(tt.expectedCols))

				// Verify column names match expected
				for i, expectedCol := range tt.expectedCols {
					if i < len(colDefs) {
						assert.Equal(t, expectedCol, colDefs[i].Name, "Column name mismatch at position %d", i)
						assert.Equal(t, expectedCol, colDefs[i].OriginalName, "Original name should match name for position %d", i)
					}
				}

				// Verify all column definitions have data types
				for i, colDef := range colDefs {
					assert.NotEmpty(t, colDef.DataType, "Column %d (%s) should have a data type", i, colDef.Name)
				}
			}
		})
	}
}

func TestGetColumnDefsForQuery_EdgeCases(t *testing.T) {
	// Create a temporary DuckDB instance for testing
	db, err := database.NewDuckDb(database.WithTempDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	// Create test tables with sample data
	setupTestTables(t, db)

	tests := []struct {
		name        string
		query       string
		description string
		expectError bool
	}{
		{
			name:        "empty query",
			query:       "",
			description: "Empty query should return error",
			expectError: true,
		},
		{
			name:        "whitespace only query",
			query:       "   \t\n   ",
			description: "Whitespace-only query should return error",
			expectError: true,
		},
		{
			name:        "query with comments",
			query:       "SELECT id, name -- comment\nFROM test_table /* another comment */",
			description: "Query with comments should work",
			expectError: false,
		},
		{
			name:        "query with special characters in column names",
			query:       "SELECT id as \"user-id\", name as \"user_name\", value as \"score_value\" FROM test_table",
			description: "Query with quoted column names should work",
			expectError: false,
		},
		{
			name:        "query with numeric column names",
			query:       "SELECT 1 as \"1\", 2 as \"2\", 3 as \"3\" FROM test_table",
			description: "Query with numeric column names should work",
			expectError: false,
		},
		{
			name:        "query with very long column names",
			query:       "SELECT id as \"very_long_column_name_that_exceeds_normal_length_limits_for_testing_purposes\", name, value FROM test_table",
			description: "Query with very long column names should work",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colDefs, err := GetColumnDefsForQuery(tt.query, db)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, colDefs)
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, colDefs)
				assert.Greater(t, len(colDefs), 0, "Should return at least one column definition")
			}
		})
	}
}

func TestGetColumnDefsForQuery_DataTypes(t *testing.T) {
	// Create a temporary DuckDB instance for testing
	db, err := database.NewDuckDb(database.WithTempDir(t.TempDir()))
	require.NoError(t, err)
	defer db.Close()

	// Create test table with various data types
	_, err = db.Exec(`
		CREATE TABLE data_types_test (
			id INTEGER,
			name VARCHAR,
			value DOUBLE,
			is_active BOOLEAN,
			created_at TIMESTAMP,
			data BLOB,
			json_data JSON,
			uuid_val UUID
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO data_types_test VALUES 
		(1, 'test', 3.14, true, '2024-01-01 10:00:00', 'binary_data', '{"key": "value"}', '123e4567-e89b-12d3-a456-426614174000')
	`)
	require.NoError(t, err)

	tests := []struct {
		name          string
		query         string
		expectedTypes []string
	}{
		{
			name:          "all data types",
			query:         "SELECT * FROM data_types_test",
			expectedTypes: []string{"INTEGER", "VARCHAR", "DOUBLE", "BOOLEAN", "TIMESTAMP", "BLOB", "JSON", "UUID"},
		},
		{
			name:          "type casting",
			query:         "SELECT CAST(id AS BIGINT) as big_id, CAST(value AS DECIMAL(10,2)) as decimal_value FROM data_types_test",
			expectedTypes: []string{"BIGINT", "DECIMAL(10,2)"},
		},
		{
			name:          "string functions",
			query:         "SELECT UPPER(name) as upper_name, LENGTH(name) as name_length, SUBSTRING(name, 1, 2) as name_sub FROM data_types_test",
			expectedTypes: []string{"VARCHAR", "BIGINT", "VARCHAR"},
		},
		{
			name:          "numeric functions",
			query:         "SELECT ABS(value) as abs_value, ROUND(value, 2) as rounded_value, CEIL(value) as ceil_value FROM data_types_test",
			expectedTypes: []string{"DOUBLE", "DOUBLE", "DOUBLE"},
		},
		{
			name:          "date functions",
			query:         "SELECT YEAR(created_at) as year, MONTH(created_at) as month, DAY(created_at) as day FROM data_types_test",
			expectedTypes: []string{"BIGINT", "BIGINT", "BIGINT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colDefs, err := GetColumnDefsForQuery(tt.query, db)
			require.NoError(t, err)
			require.Len(t, colDefs, len(tt.expectedTypes))

			for i, expectedType := range tt.expectedTypes {
				// DuckDB might return slightly different type names, so we check if the type contains our expected type
				assert.Contains(t, colDefs[i].DataType, expectedType,
					"Column %d (%s) should have data type containing %s, got %s",
					i, colDefs[i].Name, expectedType, colDefs[i].DataType)
			}
		})
	}
}

// setupTestTables creates test tables with sample data for testing
func setupTestTables(t testing.TB, db *database.DuckDb) {
	// Create test_table
	_, err := db.Exec(`
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR,
			value DOUBLE
		)
	`)
	require.NoError(t, err)

	// Create category_table
	_, err = db.Exec(`
		CREATE TABLE category_table (
			id INTEGER,
			category VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert test data into test_table
	_, err = db.Exec(`
		INSERT INTO test_table VALUES 
		(1, 'Alice', 10.5),
		(2, 'Bob', 7.2),
		(3, 'Charlie', 15.8),
		(4, 'David', 3.1),
		(5, 'Eve', 12.3)
	`)
	require.NoError(t, err)

	// Insert test data into category_table
	_, err = db.Exec(`
		INSERT INTO category_table VALUES 
		(1, 'A'),
		(2, 'B'),
		(3, 'A'),
		(4, 'C'),
		(5, 'B')
	`)
	require.NoError(t, err)
}

// BenchmarkGetColumnDefsForQuery benchmarks the GetColumnDefsForQuery function
func BenchmarkGetColumnDefsForQuery(b *testing.B) {
	// Create a temporary DuckDB instance for benchmarking
	db, err := database.NewDuckDb(database.WithTempDir(b.TempDir()))
	require.NoError(b, err)
	defer db.Close()

	// Setup test tables
	setupTestTables(b, db)

	queries := []string{
		"SELECT id, name, value FROM test_table",
		"SELECT t1.id, t1.name, t2.category FROM test_table t1 INNER JOIN category_table t2 ON t1.id = t2.id",
		"SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM test_table t1 JOIN category_table t2 ON t1.id = t2.id GROUP BY category",
		"SELECT id, name, value, ROW_NUMBER() OVER (ORDER BY value DESC) as rank FROM test_table",
	}

	for i, query := range queries {
		b.Run(fmt.Sprintf("Query_%d", i+1), func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				_, err := GetColumnDefsForQuery(query, db)
				if err != nil {
					b.Fatalf("GetColumnDefsForQuery failed: %v", err)
				}
			}
		})
	}
}
