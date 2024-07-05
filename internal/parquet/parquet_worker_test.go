package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

var db *duckDb

const testDir = "buildViewQuery_test_data"

// we use the same path for all tests
var jsonlFilePath string

func setup() error {
	var err error
	db, err = newDuckDb()
	// make tempdata directory in local folder
	// Create the directory
	err = os.MkdirAll(testDir, 0755)
	if err != nil {
		db.Close()
		return fmt.Errorf("error creating temp directory: %w", err)
	}

	// resolve the jsonl file path
	jsonlFilePath, err = filepath.Abs(filepath.Join(testDir, "test.jsonl"))
	return nil
}

func teardown() {
	os.RemoveAll("test_data")
	if db != nil {
		db.Close()
	}
}

func Test_buildViewQuery(t *testing.T) {
	if err := setup(); err != nil {
		t.Fatalf("error setting up test: %s", err)
	}
	defer teardown()

	type args struct {
		schema    *schema.RowSchema
		json      string
		sqlColumn string
	}
	tests := []struct {
		name      string
		args      args
		wantQuery string
		wantData  any
	}{
		/*
		       c.Type = "BOOLEAN"
		       c.Type = "TINYINT"
		       c.Type = "SMALLINT"
		       c.Type = "INTEGER"
		       c.Type = "BIGINT"
		       c.Type = "UTINYINT"
		       c.Type = "USMALLINT"
		       c.Type = "UINTEGER"
		       c.Type = "UBIGINT"
		       c.Type = "FLOAT"
		       c.Type = "DOUBLE"
		       c.Type = "VARCHAR"
		           c.Type = "TIMESTAMP"

		   c.Type = "BLOB"
		       c.Type = "ARRAY"
		       c.Type = "STRUCT"
		       c.Type = "MAP"
		*/
		{
			name: "struct",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{
							SourceName: "StructField",
							ColumnName: "struct_field",
							Type:       "STRUCT",
							StructFields: []*schema.ColumnSchema{
								{SourceName: "StructStringField", ColumnName: "struct_string_field", Type: "VARCHAR"},
								{SourceName: "StructIntField", ColumnName: "struct_int_field", Type: "BIGINT"},
							},
						},
					},
				},
				json:      `{  "StructField": {   "StructStringField": "StructStringVal", "StructIntField": 100   }}`,
				sqlColumn: "struct_field.struct_string_field",
			},
			wantQuery: fmt.Sprintf(`SELECT
	struct_pack(
		struct_string_field := StructField.StructStringField::VARCHAR,
		struct_int_field := StructField.StructIntField::BIGINT
	) AS struct_field
FROM
	read_json_auto('%s', format='newline_delimited')`, jsonlFilePath),
			wantData: "StructStringVal",
		},
		{
			name: "nested struct",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{
							SourceName: "StructField",
							ColumnName: "struct_field",
							Type:       "STRUCT",
							StructFields: []*schema.ColumnSchema{
								{
									SourceName: "NestedStruct",
									ColumnName: "nested_struct",
									Type:       "STRUCT",
									StructFields: []*schema.ColumnSchema{
										{
											SourceName: "NestedStructStringField",
											ColumnName: "nested_struct_string_field",
											Type:       "VARCHAR",
										},
									},
								},
								{
									SourceName: "StructStringField",
									ColumnName: "struct_string_field",
									Type:       "VARCHAR",
								},
							},
						},
					},
				},
				json:      `{  "StructField": {    "NestedStruct": {      "NestedStructStringField": "NestedStructStringVal"    },    "StructStringField": "StructStringVal"  }}`,
				sqlColumn: "struct_field.nested_struct.nested_struct_string_field",
			},
			wantQuery: fmt.Sprintf(`SELECT
	struct_pack(
		nested_struct := struct_pack(
			nested_struct_string_field := StructField.NestedStruct.NestedStructStringField::VARCHAR
		),
		struct_string_field := StructField.StructStringField::VARCHAR
	) AS struct_field
FROM
	read_json_auto('%s', format='newline_delimited')`, jsonlFilePath),
			wantData: "NestedStructStringVal",
		},
		{
			name: "scalar types",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{SourceName: "BooleanField", ColumnName: "boolean_field", Type: "BOOLEAN"},
						{SourceName: "TinyIntField", ColumnName: "tinyint_field", Type: "TINYINT"},
						{SourceName: "SmallIntField", ColumnName: "smallint_field", Type: "SMALLINT"},
						{SourceName: "IntegerField", ColumnName: "integer_field", Type: "INTEGER"},
						{SourceName: "BigIntField", ColumnName: "bigint_field", Type: "BIGINT"},
						{SourceName: "UTinyIntField", ColumnName: "utinyint_field", Type: "UTINYINT"},
						{SourceName: "USmallIntField", ColumnName: "usmallint_field", Type: "USMALLINT"},
						{SourceName: "UIntegerField", ColumnName: "uinteger_field", Type: "UINTEGER"},
						{SourceName: "UBigIntField", ColumnName: "ubigint_field", Type: "UBIGINT"},
						{SourceName: "FloatField", ColumnName: "float_field", Type: "FLOAT"},
						{SourceName: "DoubleField", ColumnName: "double_field", Type: "DOUBLE"},
						{SourceName: "VarcharField", ColumnName: "varchar_field", Type: "VARCHAR"},
						{SourceName: "TimestampField", ColumnName: "timestamp_field", Type: "TIMESTAMP"},
					},
				},
				json:      `{"BooleanField": true, "TinyIntField": 1, "SmallIntField": 2, "IntegerField": 3, "BigIntField": 4, "UTinyIntField": 5, "USmallIntField": 6, "UIntegerField": 7, "UBigIntField": 8, "FloatField": 1.23, "DoubleField": 4.56, "VarcharField": "StringValue", "TimestampField": "2024-01-01T00:00:00Z"}`,
				sqlColumn: "varchar_field",
			},
			wantQuery: fmt.Sprintf(`SELECT
	BooleanField::BOOLEAN AS boolean_field,
	TinyIntField::TINYINT AS tinyint_field,
	SmallIntField::SMALLINT AS smallint_field,
	IntegerField::INTEGER AS integer_field,
	BigIntField::BIGINT AS bigint_field,
	UTinyIntField::UTINYINT AS utinyint_field,
	USmallIntField::USMALLINT AS usmallint_field,
	UIntegerField::UINTEGER AS uinteger_field,
	UBigIntField::UBIGINT AS ubigint_field,
	FloatField::FLOAT AS float_field,
	DoubleField::DOUBLE AS double_field,
	VarcharField::VARCHAR AS varchar_field,
	TimestampField::TIMESTAMP AS timestamp_field
FROM
	read_json_auto('%s', format='newline_delimited')`, jsonlFilePath),
			wantData: "StringValue",
		},
		{
			name: "array types",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{SourceName: "BooleanArrayField", ColumnName: "boolean_array_field", Type: "BOOLEAN[]"},
						{SourceName: "TinyIntArrayField", ColumnName: "tinyint_array_field", Type: "TINYINT[]"},
						{SourceName: "SmallIntArrayField", ColumnName: "smallint_array_field", Type: "SMALLINT[]"},
						{SourceName: "IntegerArrayField", ColumnName: "integer_array_field", Type: "INTEGER[]"},
						{SourceName: "BigIntArrayField", ColumnName: "bigint_array_field", Type: "BIGINT[]"},
						{SourceName: "UTinyIntArrayField", ColumnName: "utinyint_array_field", Type: "UTINYINT[]"},
						{SourceName: "USmallIntArrayField", ColumnName: "usmallint_array_field", Type: "USMALLINT[]"},
						{SourceName: "UIntegerArrayField", ColumnName: "uinteger_array_field", Type: "UINTEGER[]"},
						{SourceName: "UBigIntArrayField", ColumnName: "ubigint_array_field", Type: "UBIGINT[]"},
						{SourceName: "FloatArrayField", ColumnName: "float_array_field", Type: "FLOAT[]"},
						{SourceName: "DoubleArrayField", ColumnName: "double_array_field", Type: "DOUBLE[]"},
						{SourceName: "VarcharArrayField", ColumnName: "varchar_array_field", Type: "VARCHAR[]"},
						{SourceName: "TimestampArrayField", ColumnName: "timestamp_array_field", Type: "TIMESTAMP[]"},
					},
				},
				json:      `{"BooleanArrayField": [true, false], "TinyIntArrayField": [1, 2], "SmallIntArrayField": [2, 3], "IntegerArrayField": [3, 4], "BigIntArrayField": [4, 5], "UTinyIntArrayField": [5, 6], "USmallIntArrayField": [6, 7], "UIntegerArrayField": [7, 8], "UBigIntArrayField": [8, 9], "FloatArrayField": [1.23, 2.34], "DoubleArrayField": [4.56, 5.67], "VarcharArrayField": ["StringValue1", "StringValue2"], "TimestampArrayField": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]}`,
				sqlColumn: "boolean_array_field",
			},
			wantQuery: fmt.Sprintf(`SELECT
	BooleanArrayField::BOOLEAN[] AS boolean_array_field,
	TinyIntArrayField::TINYINT[] AS tinyint_array_field,
	SmallIntArrayField::SMALLINT[] AS smallint_array_field,
	IntegerArrayField::INTEGER[] AS integer_array_field,
	BigIntArrayField::BIGINT[] AS bigint_array_field,
	UTinyIntArrayField::UTINYINT[] AS utinyint_array_field,
	USmallIntArrayField::USMALLINT[] AS usmallint_array_field,
	UIntegerArrayField::UINTEGER[] AS uinteger_array_field,
	UBigIntArrayField::UBIGINT[] AS ubigint_array_field,
	FloatArrayField::FLOAT[] AS float_array_field,
	DoubleArrayField::DOUBLE[] AS double_array_field,
	VarcharArrayField::VARCHAR[] AS varchar_array_field,
	TimestampArrayField::TIMESTAMP[] AS timestamp_array_field
FROM
	read_json_auto('%s', format='newline_delimited')`, jsonlFilePath),
			wantData: []any{true, false},
		},
		{
			name: "array of simple structs",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{
							SourceName: "StructArrayField",
							ColumnName: "struct_array_field",
							Type:       "STRUCT[]",
							StructFields: []*schema.ColumnSchema{
								{SourceName: "StructStringField", ColumnName: "struct_string_field", Type: "VARCHAR"},
								{SourceName: "StructIntField", ColumnName: "struct_int_field", Type: "INTEGER"},
							},
						},
					},
				},
				json:      `{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}`,
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: fmt.Sprintf(`WITH sl AS (
	SELECT
		row_number() OVER () AS rowid,
		UNNEST(COALESCE(StructArrayField, ARRAY[]::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[])::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]) AS StructArrayField
	FROM
		read_json_auto('%s', format='newline_delimited')
), json_data AS (
	SELECT
		rowid,
		StructArrayField->>'StructStringField' AS StructArrayField_StructStringField,
		StructArrayField->>'StructIntField' AS StructArrayField_StructIntField
	FROM
		sl
)
SELECT
	array_agg(struct_pack(
		struct_string_field := StructArrayField_StructStringField::VARCHAR,
		struct_int_field := StructArrayField_StructIntField::INTEGER
	)) AS struct_array_field
FROM
	json_data
JOIN
	sl ON json_data.rowid = sl.rowid
GROUP BY
	sl.rowid`, jsonlFilePath),
			wantData: "StringValue2",
		},

		{
			name: "array of simple structs plus other fields",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{
							SourceName: "StructArrayField",
							ColumnName: "struct_array_field",
							Type:       "STRUCT[]",
							StructFields: []*schema.ColumnSchema{
								{SourceName: "StructStringField", ColumnName: "struct_string_field", Type: "VARCHAR"},
								{SourceName: "StructIntField", ColumnName: "struct_int_field", Type: "INTEGER"},
							},
						},
						{SourceName: "IntField", ColumnName: "int_field", Type: "INTEGER"},
						{SourceName: "StringField", ColumnName: "string_field", Type: "VARCHAR"},
						{SourceName: "FloatField", ColumnName: "float_field", Type: "FLOAT"},
						{SourceName: "BooleanField", ColumnName: "boolean_field", Type: "BOOLEAN"},
						{
							SourceName: "IntArrayField",
							ColumnName: "int_array_field",
							Type:       "INTEGER[]",
						},
						{
							SourceName: "StringArrayField",
							ColumnName: "string_array_field",
							Type:       "VARCHAR[]",
						},
						{
							SourceName: "FloatArrayField",
							ColumnName: "float_array_field",
							Type:       "FLOAT[]",
						},
						{
							SourceName: "BooleanArrayField",
							ColumnName: "boolean_array_field",
							Type:       "BOOLEAN[]",
						},
					},
				},

				json:      `{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}], "IntField": 10, "StringField": "SampleString", "FloatField": 10.5, "BooleanField": true, "IntArrayField": [1, 2, 3], "StringArrayField": ["String1", "String2"], "FloatArrayField": [1.1, 2.2, 3.3], "BooleanArrayField": [true, false, true]}`,
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: fmt.Sprintf(`WITH sl AS (
	SELECT
		row_number() OVER () AS rowid,
		UNNEST(COALESCE(StructArrayField, ARRAY[]::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[])::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]) AS StructArrayField,
		IntField::INTEGER AS int_field,
		StringField::VARCHAR AS string_field,
		FloatField::FLOAT AS float_field,
		BooleanField::BOOLEAN AS boolean_field,
		IntArrayField::INTEGER[] AS int_array_field,
		StringArrayField::VARCHAR[] AS string_array_field,
		FloatArrayField::FLOAT[] AS float_array_field,
		BooleanArrayField::BOOLEAN[] AS boolean_array_field
	FROM
		read_json_auto('%s', format='newline_delimited')
), json_data AS (
	SELECT
		rowid,
		StructArrayField->>'StructStringField' AS StructArrayField_StructStringField,
		StructArrayField->>'StructIntField' AS StructArrayField_StructIntField
	FROM
		sl
)
SELECT
	array_agg(struct_pack(
		struct_string_field := StructArrayField_StructStringField::VARCHAR,
		struct_int_field := StructArrayField_StructIntField::INTEGER
	)) AS struct_array_field,
	sl.int_field,
	sl.string_field,
	sl.float_field,
	sl.boolean_field,
	sl.int_array_field,
	sl.string_array_field,
	sl.float_array_field,
	sl.boolean_array_field
FROM
	json_data
JOIN
	sl ON json_data.rowid = sl.rowid
GROUP BY
	sl.rowid, sl.int_field, sl.string_field, sl.float_field, sl.boolean_field, sl.int_array_field, sl.string_array_field, sl.float_array_field, sl.boolean_array_field`, jsonlFilePath),
			wantData: "StringValue2",
		},
		{
			name: "array of simple structs with null value",
			args: args{
				schema: &schema.RowSchema{
					Columns: []*schema.ColumnSchema{
						{
							SourceName: "StructArrayField",
							ColumnName: "struct_array_field",
							Type:       "STRUCT[]",
							StructFields: []*schema.ColumnSchema{
								{SourceName: "StructStringField", ColumnName: "struct_string_field", Type: "VARCHAR"},
								{SourceName: "StructIntField", ColumnName: "struct_int_field", Type: "INTEGER"},
							},
						},
					},
				},
				json:      `{"StructArrayField": null}`,
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: fmt.Sprintf(`WITH sl AS (
	SELECT
		row_number() OVER () AS rowid,
		UNNEST(COALESCE(StructArrayField, ARRAY[]::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[])::STRUCT(StructStringField VARCHAR, StructIntField INTEGER)[]) AS StructArrayField
	FROM
		read_json_auto('%s', format='newline_delimited')
), json_data AS (
	SELECT
		rowid,
		StructArrayField->>'StructStringField' AS StructArrayField_StructStringField,
		StructArrayField->>'StructIntField' AS StructArrayField_StructIntField
	FROM
		sl
)
SELECT
	array_agg(struct_pack(
		struct_string_field := StructArrayField_StructStringField::VARCHAR,
		struct_int_field := StructArrayField_StructIntField::INTEGER
	)) AS struct_array_field
FROM
	json_data
JOIN
	sl ON json_data.rowid = sl.rowid
GROUP BY
	sl.rowid`, jsonlFilePath),
			wantData: nil,
		},

		//TODO doesn't work
		//		{
		//			name: "map types",
		//			args: args{
		//				schema: &schema.RowSchema{
		//					Columns: []*schema.ColumnSchema{
		//						{SourceName: "BooleanMapField", ColumnName: "boolean_map_field", Type: "MAP<VARCHAR, BOOLEAN>"},
		//						{SourceName: "TinyIntMapField", ColumnName: "tinyint_map_field", Type: "MAP<VARCHAR, TINYINT>"},
		//						{SourceName: "SmallIntMapField", ColumnName: "smallint_map_field", Type: "MAP<VARCHAR, SMALLINT>"},
		//						{SourceName: "IntegerMapField", ColumnName: "integer_map_field", Type: "MAP<VARCHAR, INTEGER>"},
		//						{SourceName: "BigIntMapField", ColumnName: "bigint_map_field", Type: "MAP<VARCHAR, BIGINT>"},
		//						{SourceName: "FloatMapField", ColumnName: "float_map_field", Type: "MAP<VARCHAR, FLOAT>"},
		//						{SourceName: "DoubleMapField", ColumnName: "double_map_field", Type: "MAP<VARCHAR, DOUBLE>"},
		//						{SourceName: "VarcharMapField", ColumnName: "varchar_map_field", Type: "MAP<VARCHAR, VARCHAR>"},
		//						{SourceName: "TimestampMapField", ColumnName: "timestamp_map_field", Type: "MAP<VARCHAR, TIMESTAMP>"},
		//					},
		//				},
		//				json:      `{"BooleanMapField": {"key1": true, "key2": false}, "TinyIntMapField": {"key1": 1, "key2": 2}, "SmallIntMapField": {"key1": 2, "key2": 3}, "IntegerMapField": {"key1": 3, "key2": 4}, "BigIntMapField": {"key1": 4, "key2": 5}, "FloatMapField": {"key1": 1.23, "key2": 2.34}, "DoubleMapField": {"key1": 4.56, "key2": 5.67}, "VarcharMapField": {"key1": "StringValue1", "key2": "StringValue2"}, "TimestampMapField": {"key1": "2024-01-01T00:00:00Z", "key2": "2024-01-02T00:00:00Z"}}`,
		//				sqlColumn: "boolean_map_field",
		//			},
		//			wantQuery: fmt.Sprintf(`SELECT
		//	json_extract(json, '$.BooleanMapField')::MAP(VARCHAR, BOOLEAN> AS boolean_map_field,
		//	json_extract(json, '$.TinyIntMapField')::MAP(VARCHAR, TINYINT> AS tinyint_map_field,
		//	json_extract(json, '$.SmallIntMapField')::MAP(VARCHAR, SMALLINT) AS smallint_map_field,
		//	json_extract(json, '$.IntegerMapField')::MAP(VARCHAR, INTEGER) AS integer_map_field,
		//	json_extract(json, '$.BigIntMapField')::MAP(VARCHAR, BIGINT) AS bigint_map_field,
		//	json_extract(json, '$.FloatMapField')::MAP(VARCHAR, FLOAT) AS float_map_field,
		//	json_extract(json, '$.DoubleMapField')::MAP(VARCHAR, DOUBLE) AS double_map_field,
		//	json_extract(json, '$.VarcharMapField')::MAP(VARCHAR, VARCHAR) AS varchar_map_field,
		//	json_extract(json, '$.TimestampMapField')::MAP(VARCHAR, TIMESTAMP) AS timestamp_map_field
		//FROM read_json_auto('%s', format='newline_delimited')`, jsonlFilePath),
		//			wantData: map[string]bool{"key1": true, "key2": false},
		//		},
	}

	defer os.RemoveAll("test_data")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := buildViewQuery(tt.args.schema, jsonlFilePath)

			// first check the quey is as expected
			if query != tt.wantQuery {
				t.Errorf("buildViewQuery(), got:\n%s\nwant:\n%s", query, tt.wantQuery)
			}

			gotData, err := executeQuery(t, query, tt.args.json, tt.args.sqlColumn)
			if err != nil {
				t.Errorf("error executing query: %s", err)
			} else if !reflect.DeepEqual(gotData, tt.wantData) {
				t.Errorf("buildViewQuery() query returned %v, want %v", gotData, tt.wantData)
			}
		})
	}
}

func executeQuery(t *testing.T, query, json, sqlColumn string) (any, error) {
	// now verify the query runs
	// copy json to a jsonl file
	err := createJSONLFile(json)
	if err != nil {
		t.Fatalf("error creating jsonl file: %s", err)
	}
	defer os.Remove(jsonlFilePath)

	// execute in duckdb
	// build select query
	testQuery := fmt.Sprintf("SELECT %s from (%s)", sqlColumn, query)
	row := db.QueryRow(testQuery)

	// get the data
	var data any
	err = row.Scan(&data)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return nil, fmt.Errorf("error scanning data: %w", err)
	}
	return data, nil

}

func createJSONLFile(json string) error {
	// remove just in case
	os.Remove(jsonlFilePath)
	jsonlFile, err := os.Create(jsonlFilePath)
	if err != nil {
		return fmt.Errorf("error creating jsonl file: %w", err)
	}
	_, err = jsonlFile.WriteString(json)
	if err != nil {
		return fmt.Errorf("error writing to jsonl file: %w", err)
	}
	// close the file
	err = jsonlFile.Close()
	if err != nil {
		return fmt.Errorf("error closing jsonl file: %w", err)
	}
	return err
}
