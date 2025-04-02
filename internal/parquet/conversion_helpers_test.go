package parquet

import (
	_ "github.com/marcboeker/go-duckdb/v2"
)

//
//func TestSelectClauseGeneration(t *testing.T) {
//	tests := []struct {
//		name           string
//		input          string
//		columnType     string
//		sourceName     string
//		columnName     string
//		expectedSQL    string
//		expectedDbType string
//		expectedValue  interface{}
//	}{
//		// Integer types - DuckDB returns specific int types through database/sql
//		{
//			name:           "tinyint",
//			input:          "42",
//			columnType:     "tinyint",
//			sourceName:     "tinyint_field",
//			columnName:     "tinyint_field",
//			expectedSQL:    "\t\"tinyint_field\"::tinyint as \"tinyint_field\"",
//			expectedDbType: "tinyint",
//			expectedValue:  int8(42),
//		},
//		{
//			name:           "smallint",
//			input:          "32000",
//			columnType:     "smallint",
//			sourceName:     "smallint_field",
//			columnName:     "smallint_field",
//			expectedSQL:    "\t\"smallint_field\"::smallint as \"smallint_field\"",
//			expectedDbType: "smallint",
//			expectedValue:  int16(32000),
//		},
//		{
//			name:           "integer",
//			input:          "42",
//			columnType:     "integer",
//			sourceName:     "int_field",
//			columnName:     "int_field",
//			expectedSQL:    "\t\"int_field\"::integer as \"int_field\"",
//			expectedDbType: "integer",
//			expectedValue:  int32(42),
//		},
//		{
//			name:           "bigint",
//			input:          "9223372036854775807",
//			columnType:     "bigint",
//			sourceName:     "bigint_field",
//			columnName:     "bigint_field",
//			expectedSQL:    "\t\"bigint_field\"::bigint as \"bigint_field\"",
//			expectedDbType: "bigint",
//			expectedValue:  int64(9223372036854775807),
//		},
//		// Unsigned integer types - DuckDB returns specific uint types
//		{
//			name:           "utinyint",
//			input:          "255",
//			columnType:     "utinyint",
//			sourceName:     "utinyint_field",
//			columnName:     "utinyint_field",
//			expectedSQL:    "\t\"utinyint_field\"::utinyint as \"utinyint_field\"",
//			expectedDbType: "utinyint",
//			expectedValue:  uint8(255),
//		},
//		{
//			name:           "usmallint",
//			input:          "65535",
//			columnType:     "usmallint",
//			sourceName:     "usmallint_field",
//			columnName:     "usmallint_field",
//			expectedSQL:    "\t\"usmallint_field\"::usmallint as \"usmallint_field\"",
//			expectedDbType: "usmallint",
//			expectedValue:  uint16(65535),
//		},
//		{
//			name:           "uinteger",
//			input:          "4294967295",
//			columnType:     "uinteger",
//			sourceName:     "uint_field",
//			columnName:     "uint_field",
//			expectedSQL:    "\t\"uint_field\"::uinteger as \"uint_field\"",
//			expectedDbType: "uinteger",
//			expectedValue:  uint32(4294967295),
//		},
//		// Note: skipping ubigint as it would overflow int64
//
//		// Floating point types
//		{
//			name:           "float",
//			input:          "3.14",
//			columnType:     "float",
//			sourceName:     "float_field",
//			columnName:     "float_field",
//			expectedSQL:    "\t\"float_field\"::float as \"float_field\"",
//			expectedDbType: "float",
//			expectedValue:  float32(3.14),
//		},
//		{
//			name:           "double",
//			input:          "3.141592653589793",
//			columnType:     "double",
//			sourceName:     "double_field",
//			columnName:     "double_field",
//			expectedSQL:    "\t\"double_field\"::double as \"double_field\"",
//			expectedDbType: "double",
//			expectedValue:  float64(3.141592653589793),
//		},
//		// String types
//		{
//			name:           "varchar",
//			input:          "hello",
//			columnType:     "varchar",
//			sourceName:     "string_field",
//			columnName:     "string_field",
//			expectedSQL:    "\t\"string_field\"::varchar as \"string_field\"",
//			expectedDbType: "varchar",
//			expectedValue:  "hello",
//		},
//		// Boolean type
//		{
//			name:           "boolean",
//			input:          "true",
//			columnType:     "boolean",
//			sourceName:     "bool_field",
//			columnName:     "bool_field",
//			expectedSQL:    "\t\"bool_field\"::boolean as \"bool_field\"",
//			expectedDbType: "boolean",
//			expectedValue:  true,
//		},
//		// Date/Time types - DuckDB returns time.Time through database/sql
//		{
//			name:           "date",
//			input:          "2024-03-14",
//			columnType:     "date",
//			sourceName:     "date_field",
//			columnName:     "date_field",
//			expectedSQL:    "\t\"date_field\"::data as \"date_field\"",
//			expectedDbType: "date",
//			expectedValue:  time.Date(2024, 3, 14, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:           "time",
//			input:          "15:45:30",
//			columnType:     "TIME",
//			sourceName:     "time_field",
//			columnName:     "time_field",
//			expectedSQL:    "\t\"time_field\"::TIME as \"time_field\"",
//			expectedDbType: "TIME",
//			expectedValue:  time.Date(1, 1, 1, 15, 45, 30, 0, time.UTC),
//		},
//		{
//			name:           "timestamp",
//			input:          "2024-03-14 15:45:30",
//			columnType:     "timestamp",
//			sourceName:     "timestamp_field",
//			columnName:     "timestamp_field",
//			expectedSQL:    "\t\"timestamp_field\"::timestamp as \"timestamp_field\"",
//			expectedDbType: "timestamp",
//			expectedValue:  time.Date(2024, 3, 14, 15, 45, 30, 0, time.UTC),
//		},
//		// Array types - integer arrays come back as []int32
//		{
//			name:           "integer array",
//			input:          "1,2,3,4,5",
//			columnType:     "integer[]",
//			sourceName:     "int_array_field",
//			columnName:     "int_array_field",
//			expectedSQL:    "\tstring_split(\"int_array_field\", ',')::integer[] as \"int_array_field\"",
//			expectedDbType: "integer[]",
//			expectedValue:  []interface{}{int32(1), int32(2), int32(3), int32(4), int32(5)},
//		},
//		{
//			name:           "varchar array",
//			input:          "a,b,c,d",
//			columnType:     "varchar[]",
//			sourceName:     "string_array_field",
//			columnName:     "string_array_field",
//			expectedSQL:    "\tstring_split(\"string_array_field\", ',')::varchar[] as \"string_array_field\"",
//			expectedDbType: "varchar[]",
//			expectedValue:  []interface{}{"a", "b", "c", "d"},
//		},
//		{
//			name:           "boolean array",
//			input:          "true,false,true",
//			columnType:     "boolean[]",
//			sourceName:     "bool_array_field",
//			columnName:     "bool_array_field",
//			expectedSQL:    "\tstring_split(\"bool_array_field\", ',')::boolean[] as \"bool_array_field\"",
//			expectedDbType: "boolean[]",
//			expectedValue:  []interface{}{true, false, true},
//		},
//		// Special types
//		{
//			name:           "uuid",
//			input:          "123e4567-e89b-12d3-a456-426614174000",
//			columnType:     "UUID",
//			sourceName:     "uuid_field",
//			columnName:     "uuid_field",
//			expectedSQL:    "\t\"uuid_field\"::UUID as \"uuid_field\"",
//			expectedDbType: "UUID",
//			expectedValue:  []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00},
//		},
//		{
//			name:           "json",
//			input:          "{\"key\": \"value\"}",
//			columnType:     "json",
//			sourceName:     "json_field",
//			columnName:     "json_field",
//			expectedSQL:    "\tjson(\"json_field\") as \"json_field\"",
//			expectedDbType: "json",
//			expectedValue:  "{\"key\":\"value\"}",
//		},
//		{
//			name:           "interval",
//			input:          "1 year 2 months",
//			columnType:     "interval",
//			sourceName:     "interval_field",
//			columnName:     "interval_field",
//			expectedSQL:    "\t\"interval_field\"::interval as \"interval_field\"",
//			expectedDbType: "interval",
//			expectedValue:  "1 year 2 months", // DuckDB returns intervals in their natural format
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// Test SQL generation
//			column := &schema.ColumnSchema{
//				Type:       tt.columnType,
//				SourceName: tt.sourceName,
//				ColumnName: tt.columnName,
//			}
//			selectSql := getSelectSqlForDynamicField(column)—
//			assert.Equal(t, tt.expectedSQL, selectSql)
//
//			// Test actual DuckDB type
//			db, err := sql.Open("duckdb", "")
//			require.NoError(t, err)
//			defer db.Close()
//
//			// Create a view with the converted type
//			createViewSQL := fmt.Sprintf("CREATE VIEW test_view AS select %s from (select '%s' as %s) t",
//				selectSql, tt.input, tt.sourceName)
//			_, err = db.Exec(createViewSQL)
//			require.NoError(t, err)
//
//			// Query the column type from the view
//			var columnType string
//			err = db.QueryRow("select data_type from duckdb_columns where table_name = 'test_view' AND column_name = $1",
//				tt.columnName).Scan(&columnType)
//			require.NoError(t, err)
//			assert.Equal(t, tt.expectedDbType, columnType)
//
//			// Also test the actual value
//			var value interface{}
//			if tt.name == "interval" {
//				// For intervals, we need to cast to string to get a consistent format
//				err = db.QueryRow(fmt.Sprintf("select CAST(%s AS varchar) from test_view", tt.columnName)).Scan(&value)
//			} else {
//				err = db.QueryRow(fmt.Sprintf("select %s from test_view", tt.columnName)).Scan(&value)
//			}
//			require.NoError(t, err)
//
//			// Debug output
//			t.Logf("Test case: %s", tt.name)
//			t.Logf("Expected type: %T, value: %v", tt.expectedValue, tt.expectedValue)
//			t.Logf("Actual type: %T, value: %v", value, value)
//
//			// For floating point types, use InDelta
//			switch v := tt.expectedValue.(type) {
//			case float32:
//				assert.InDelta(t, float64(v), value, 0.0000001)
//			case float64:
//				assert.InDelta(t, v, value, 0.0000001)
//			default:
//				assert.Equal(t, tt.expectedValue, value)
//			}
//
//			// Clean up
//			_, err = db.Exec("DROP VIEW test_view")
//			require.NoError(t, err)
//		})
//	}
//}
//
//func TestSelectClauseGenerationErrors(t *testing.T) {
//	tests := []struct {
//		name           string
//		input          string
//		columnType     string
//		sourceName     string
//		columnName     string
//		expectedSQL    string
//		expectedDbType string
//		expectedError  string
//	}{
//		// Integer types - out of range values
//		{
//			name:           "tinyint overflow",
//			input:          "128", // tinyint max is 127
//			columnType:     "tinyint",
//			sourceName:     "tinyint_field",
//			columnName:     "tinyint_field",
//			expectedSQL:    "\t\"tinyint_field\"::tinyint as \"tinyint_field\"",
//			expectedDbType: "tinyint",
//			expectedError:  "Could not convert string '128' to INT8",
//		},
//		{
//			name:           "smallint overflow",
//			input:          "32768", // smallint max is 32767
//			columnType:     "smallint",
//			sourceName:     "smallint_field",
//			columnName:     "smallint_field",
//			expectedSQL:    "\t\"smallint_field\"::smallint as \"smallint_field\"",
//			expectedDbType: "smallint",
//			expectedError:  "Could not convert string '32768' to INT16",
//		},
//		// Invalid numeric strings
//		{
//			name:           "invalid integer",
//			input:          "not_a_number",
//			columnType:     "integer",
//			sourceName:     "int_field",
//			columnName:     "int_field",
//			expectedSQL:    "\t\"int_field\"::integer as \"int_field\"",
//			expectedDbType: "integer",
//			expectedError:  "Could not convert string 'not_a_number' to INT32",
//		},
//		{
//			name:           "invalid float",
//			input:          "not_a_float",
//			columnType:     "float",
//			sourceName:     "float_field",
//			columnName:     "float_field",
//			expectedSQL:    "\t\"float_field\"::float as \"float_field\"",
//			expectedDbType: "float",
//			expectedError:  "Could not convert string 'not_a_float' to float",
//		},
//		// Invalid date/time formats
//		{
//			name:           "invalid date",
//			input:          "2024-13-45", // Invalid month and day
//			columnType:     "date",
//			sourceName:     "date_field",
//			columnName:     "date_field",
//			expectedSQL:    "\t\"date_field\"::data as \"date_field\"",
//			expectedDbType: "date",
//			expectedError:  "date field value out of range: \"2024-13-45\", expected format is (YYYY-MM-DD)",
//		},
//		{
//			name:           "invalid time",
//			input:          "25:70:99", // Invalid hours, minutes, seconds
//			columnType:     "TIME",
//			sourceName:     "time_field",
//			columnName:     "time_field",
//			expectedSQL:    "\t\"time_field\"::TIME as \"time_field\"",
//			expectedDbType: "TIME",
//			expectedError:  "time field value out of range: \"25:70:99\", expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS])",
//		},
//		{
//			name:           "invalid timestamp",
//			input:          "2024-13-45 25:70:99", // Invalid date and time
//			columnType:     "timestamp",
//			sourceName:     "timestamp_field",
//			columnName:     "timestamp_field",
//			expectedSQL:    "\t\"timestamp_field\"::timestamp as \"timestamp_field\"",
//			expectedDbType: "timestamp",
//			expectedError:  "timestamp field value out of range: \"2024-13-45 25:70:99\", expected format is (YYYY-MM-DD HH:MM:SS[.US][±HH:MM| ZONE])",
//		},
//		// Invalid boolean values
//		{
//			name:           "invalid boolean",
//			input:          "not_a_boolean",
//			columnType:     "boolean",
//			sourceName:     "bool_field",
//			columnName:     "bool_field",
//			expectedSQL:    "\t\"bool_field\"::boolean as \"bool_field\"",
//			expectedDbType: "boolean",
//			expectedError:  "Could not convert string 'not_a_boolean' to BOOL",
//		},
//		// Invalid array values
//		{
//			name:           "invalid integer array",
//			input:          "1,not_a_number,3",
//			columnType:     "integer[]",
//			sourceName:     "int_array_field",
//			columnName:     "int_array_field",
//			expectedSQL:    "\tstring_split(\"int_array_field\", ',')::integer[] as \"int_array_field\"",
//			expectedDbType: "integer[]",
//			expectedError:  "Could not convert string 'not_a_number' to INT32",
//		},
//		// Invalid UUID format
//		{
//			name:           "invalid uuid",
//			input:          "not-a-valid-uuid",
//			columnType:     "UUID",
//			sourceName:     "uuid_field",
//			columnName:     "uuid_field",
//			expectedSQL:    "\t\"uuid_field\"::UUID as \"uuid_field\"",
//			expectedDbType: "UUID",
//			expectedError:  "Could not convert string 'not-a-valid-uuid' to INT128",
//		},
//		// Invalid JSON format
//		{
//			name:           "invalid json",
//			input:          "{not_valid_json",
//			columnType:     "json",
//			sourceName:     "json_field",
//			columnName:     "json_field",
//			expectedSQL:    "\tjson(\"json_field\") as \"json_field\"",
//			expectedDbType: "json",
//			expectedError:  "Malformed JSON",
//		},
//		// Invalid interval format
//		{
//			name:           "invalid interval",
//			input:          "not a valid interval",
//			columnType:     "interval",
//			sourceName:     "interval_field",
//			columnName:     "interval_field",
//			expectedSQL:    "\t\"interval_field\"::interval as \"interval_field\"",
//			expectedDbType: "interval",
//			expectedError:  "Could not convert string 'not a valid interval' to interval",
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// Test SQL generation
//			column := &schema.ColumnSchema{
//				Type:       tt.columnType,
//				SourceName: tt.sourceName,
//				ColumnName: tt.columnName,
//			}
//			selectSql := getSelectSqlForDynamicField(column)
//			assert.Equal(t, tt.expectedSQL, selectSql)
//
//			// Test actual DuckDB type and error handling
//			db, err := sql.Open("duckdb", "")
//			require.NoError(t, err)
//			defer db.Close()
//
//			// Create a view with the converted type
//			createViewSQL := fmt.Sprintf("CREATE VIEW test_view AS select %s from (select '%s' as %s) t",
//				selectSql, tt.input, tt.sourceName)
//			_, err = db.Exec(createViewSQL)
//			if err == nil {
//				// If view creation succeeded, try to query it
//				var value interface{}
//				err = db.QueryRow(fmt.Sprintf("select %s from test_view", tt.columnName)).Scan(&value)
//
//				// Clean up the view regardless of the query result
//				_, cleanupErr := db.Exec("DROP VIEW test_view")
//				require.NoError(t, cleanupErr)
//			}
//
//			// Verify that we got an error and it contains the expected message
//			require.Error(t, err)
//			assert.Contains(t, err.Error(), tt.expectedError)
//		})
//	}
//}
//
//func TestTimeFormatParsing(t *testing.T) {
//	tests := []struct {
//		name          string
//		input         string
//		columnType    string
//		timeFormat    string
//		sourceName    string
//		columnName    string
//		expectedSQL   string
//		expectedValue time.Time
//		expectedError string
//	}{
//		// Tests without explicit format - only ISO formats work automatically
//		{
//			name:          "iso timestamp no format",
//			input:         "2024-03-14 15:45:30",
//			columnType:    "timestamp",
//			timeFormat:    "",
//			sourceName:    "ts_field",
//			columnName:    "ts_field",
//			expectedSQL:   "\t\"ts_field\"::timestamp as \"ts_field\"",
//			expectedValue: time.Date(2024, 3, 14, 15, 45, 30, 0, time.UTC),
//		},
//		{
//			name:          "iso date no format",
//			input:         "2024-03-14",
//			columnType:    "date",
//			timeFormat:    "",
//			sourceName:    "date_field",
//			columnName:    "date_field",
//			expectedSQL:   "\t\"date_field\"::data as \"date_field\"",
//			expectedValue: time.Date(2024, 3, 14, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:          "timestamp with T separator no format",
//			input:         "2024-03-14T15:45:30",
//			columnType:    "timestamp",
//			timeFormat:    "",
//			sourceName:    "ts_field",
//			columnName:    "ts_field",
//			expectedSQL:   "\t\"ts_field\"::timestamp as \"ts_field\"",
//			expectedValue: time.Date(2024, 3, 14, 15, 45, 30, 0, time.UTC),
//		},
//		{
//			name:          "timestamp with microseconds no format",
//			input:         "2024-03-14 15:45:30.123456",
//			columnType:    "timestamp",
//			timeFormat:    "",
//			sourceName:    "ts_field",
//			columnName:    "ts_field",
//			expectedSQL:   "\t\"ts_field\"::timestamp as \"ts_field\"",
//			expectedValue: time.Date(2024, 3, 14, 15, 45, 30, 123456000, time.UTC),
//		},
//		// Tests with explicit format - required for non-ISO formats
//		{
//			name:          "european date format DD/MM/YYYY",
//			input:         "14/03/2024",
//			columnType:    "date",
//			timeFormat:    "%d/%m/%Y",
//			sourceName:    "date_field",
//			columnName:    "date_field",
//			expectedSQL:   "\tstrptime(\"date_field\", '%d/%m/%Y') as \"date_field\"",
//			expectedValue: time.Date(2024, 3, 14, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:          "american date format MM/DD/YYYY",
//			input:         "03/14/2024",
//			columnType:    "date",
//			timeFormat:    "%m/%d/%Y",
//			sourceName:    "date_field",
//			columnName:    "date_field",
//			expectedSQL:   "\tstrptime(\"date_field\", '%m/%d/%Y') as \"date_field\"",
//			expectedValue: time.Date(2024, 3, 14, 0, 0, 0, 0, time.UTC),
//		},
//		{
//			name:          "custom timestamp format DD-MM-YYYY HH:MM",
//			input:         "14-03-2024 15:45",
//			columnType:    "timestamp",
//			timeFormat:    "%d-%m-%Y %H:%M",
//			sourceName:    "ts_field",
//			columnName:    "ts_field",
//			expectedSQL:   "\tstrptime(\"ts_field\", '%d-%m-%Y %H:%M') as \"ts_field\"",
//			expectedValue: time.Date(2024, 3, 14, 15, 45, 0, 0, time.UTC),
//		},
//		{
//			name:          "timestamp with timezone format",
//			input:         "2024-03-14 15:45:30+02:00",
//			columnType:    "timestamp",
//			timeFormat:    "%Y-%m-%d %H:%M:%S%z",
//			sourceName:    "ts_field",
//			columnName:    "ts_field",
//			expectedSQL:   "\tstrptime(\"ts_field\", '%Y-%m-%d %H:%M:%S%z') as \"ts_field\"",
//			expectedValue: time.Date(2024, 3, 14, 13, 45, 30, 0, time.UTC), // Note: adjusted for UTC
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// Create column schema
//			column := &schema.ColumnSchema{
//				Type:       tt.columnType,
//				SourceName: tt.sourceName,
//				ColumnName: tt.columnName,
//				TimeFormat: tt.timeFormat,
//			}
//
//			// Get SQL for the column
//			selectSql := getSelectSqlForDynamicField(column)
//			assert.Equal(t, tt.expectedSQL, selectSql)
//
//			// Test with DuckDB
//			db, err := sql.Open("duckdb", "")
//			require.NoError(t, err)
//			defer db.Close()
//
//			// Create view with the test data
//			createViewSQL := fmt.Sprintf("CREATE VIEW test_view AS select %s from (select '%s' as %s) t",
//				selectSql, tt.input, tt.sourceName)
//			_, err = db.Exec(createViewSQL)
//
//			if tt.expectedError != "" {
//				require.Error(t, err)
//				assert.Contains(t, err.Error(), tt.expectedError)
//				return
//			}
//
//			require.NoError(t, err)
//
//			// Query the value
//			var value time.Time
//			err = db.QueryRow(fmt.Sprintf("select %s from test_view", tt.columnName)).Scan(&value)
//			require.NoError(t, err)
//
//			// For debugging
//			t.Logf("Test case: %s", tt.name)
//			t.Logf("Input: %s", tt.input)
//			t.Logf("Format: %s", tt.timeFormat)
//			t.Logf("SQL: %s", selectSql)
//			t.Logf("Expected: %v", tt.expectedValue)
//			t.Logf("Got: %v", value)
//
//			assert.Equal(t, tt.expectedValue, value)
//
//			// Clean up
//			_, err = db.Exec("DROP VIEW test_view")
//			require.NoError(t, err)
//		})
//	}
//}
