package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/spf13/viper"
	pcmdconfig "github.com/turbot/pipe-fittings/v2/cmdconfig"
	"github.com/turbot/pipe-fittings/v2/parse"
	"github.com/turbot/pipe-fittings/v2/workspace_profile"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
)

var testDb *database.DuckDb

const testDir = "buildViewQuery_test_data"

// we use the same path for all tests
var jsonlFilePath string

func setup() error {
	var err error

	// Create a temporary config directory
	tempConfigDir, err := os.MkdirTemp("", "tailpipe_test_config")
	if err != nil {
		return fmt.Errorf("error creating temp config directory: %w", err)
	}

	// Set the config path to our temporary directory
	viper.Set("config_path", tempConfigDir)

	// Initialize workspace profile with parse options
	parseOpts := []parse.ParseHclOpt{
		parse.WithEscapeBackticks(true),
		parse.WithDisableTemplateForProperties(constants.GrokConfigProperties),
	}
	loader, err := pcmdconfig.GetWorkspaceProfileLoader[*workspace_profile.TailpipeWorkspaceProfile](parseOpts...)
	if err != nil {
		return fmt.Errorf("error creating workspace profile loader: %w", err)
	}
	config.GlobalWorkspaceProfile = loader.GetActiveWorkspaceProfile()
	if err := config.GlobalWorkspaceProfile.EnsureWorkspaceDirs(); err != nil {
		return fmt.Errorf("error ensuring workspace dirs: %w", err)
	}

	db, err := database.NewDuckDb(database.WithDuckDbExtensions(constants.DuckDbExtensions))
	if err != nil {
		return fmt.Errorf("error creating duckdb: %w", err)
	}
	testDb = db
	// make tempdata directory in local folder
	// Create the directory
	err = os.MkdirAll(testDir, 0755)
	if err != nil {
		db.Close()
		return fmt.Errorf("error creating temp directory: %w", err)
	}

	// resolve the jsonl file path
	jsonlFilePath, err = filepath.Abs(filepath.Join(testDir, "test.jsonl"))
	return err
}

func teardown() {
	os.RemoveAll("test_data")
	if testDb != nil {
		testDb.Close()
	}
}

func Test_buildViewQuery(t *testing.T) {
	// set the version explicitly here since version is set during build time
	// then set the app specific constants needed for the tests
	viper.Set("main.version", "0.0.1")
	cmdconfig.SetAppSpecificConstants()

	if err := setup(); err != nil {
		t.Fatalf("error setting up test: %s", err)
	}
	defer teardown()

	type args struct {
		schema    *schema.ConversionSchema
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
		       c.Type = "boolean"
		       c.Type = "tinyint"
		       c.Type = "smallint"
		       c.Type = "integer"
		       c.Type = "bigint"
		       c.Type = "utinyint"
		       c.Type = "usmallint"
		       c.Type = "uinteger"
		       c.Type = "ubigint"
		       c.Type = "float"
		       c.Type = "double"
		       c.Type = "varchar"
		           c.Type = "timestamp"

		   c.Type = "blob"
		       c.Type = "array"
		       c.Type = "struct"
		       c.Type = "map"
		*/
		{
			name: "struct",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{  "StructField": {   "StructStringField": "StructStringVal", "StructIntField": 100   }}`,
				sqlColumn: "struct_field.struct_string_field",
			},
			wantQuery: `select * from(select
	case
		when "StructField" is null then null
		else struct_pack(
			"struct_string_field" := "StructField"."StructStringField"::varchar,
			"struct_int_field" := "StructField"."StructIntField"::bigint
		)
	end as "struct_field"
from
	read_ndjson(
		'%s',
		columns = {
			"StructField": 'struct("StructStringField" varchar, "StructIntField" bigint)'
		}
	))`,
			wantData: []any{"StructStringVal"},
		},
		{
			name: "json",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
						Columns: []*schema.ColumnSchema{
							{
								SourceName: "JsonField",
								ColumnName: "json_field",
								Type:       "JSON",
							},
						},
					},
				},
				json:      `{  "JsonField": {   "string_field": "JsonStringVal", "int_field": 100   }}`,
				sqlColumn: "json_field.string_field",
			},
			wantQuery: `select * from(select
	json("JsonField") as "json_field"
from
	read_ndjson(
		'%s',
		columns = {
			"JsonField": 'json'
		}
	))`,
			wantData: []any{`JsonStringVal`},
		},
		{
			name: "struct with keyword names",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
						Columns: []*schema.ColumnSchema{
							{
								SourceName: "end",
								ColumnName: "end",
								Type:       "STRUCT",
								StructFields: []*schema.ColumnSchema{
									{SourceName: "any", ColumnName: "any", Type: "VARCHAR"},
								},
							},
						},
					},
				},
				json:      `{  "end": {   "any": "StructStringVal"  }}`,
				sqlColumn: `"end"."any"`,
			},
			wantQuery: `select * from(select
	case
		when "end" is null then null
		else struct_pack(
			"any" := "end"."any"::varchar
		)
	end as "end"
from
	read_ndjson(
		'%s',
		columns = {
			"end": 'struct("any" varchar)'
		}
	))`,
			wantData: []any{"StructStringVal"},
		},
		{
			name: "null struct",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
						Columns: []*schema.ColumnSchema{
							{
								SourceName: "end",
								ColumnName: "end",
								Type:       "STRUCT",
								StructFields: []*schema.ColumnSchema{
									{SourceName: "any", ColumnName: "any", Type: "VARCHAR"},
								},
							},
						},
					},
				},
				json:      `{ }`,
				sqlColumn: `"end"."any"`,
			},
			wantQuery: `select * from(select
	case
		when "end" is null then null
		else struct_pack(
			"any" := "end"."any"::varchar
		)
	end as "end"
from
	read_ndjson(
		'%s',
		columns = {
			"end": 'struct("any" varchar)'
		}
	))`,
			wantData: []any{nil},
		},
		{
			name: "nested struct",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{  "StructField": {    "NestedStruct": {      "NestedStructStringField": "NestedStructStringVal"    },    "StructStringField": "StructStringVal"  }}`,
				sqlColumn: "struct_field.nested_struct.nested_struct_string_field",
			},
			wantQuery: `select * from(select
	case
		when "StructField" is null then null
		else struct_pack(
			"nested_struct" := case
				when "StructField"."NestedStruct" is null then null
				else struct_pack(
					"nested_struct_string_field" := "StructField"."NestedStruct"."NestedStructStringField"::varchar
				)
			end,
			"struct_string_field" := "StructField"."StructStringField"::varchar
		)
	end as "struct_field"
from
	read_ndjson(
		'%s',
		columns = {
			"StructField": 'struct("NestedStruct" struct("NestedStructStringField" varchar), "StructStringField" varchar)'
		}
	))`,
			wantData: []any{"NestedStructStringVal"},
		},
		{
			name: "null nested struct",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json: `{  "StructField": {    "NestedStruct": {      "NestedStructStringField": "NestedStructStringVal"    },    "StructStringField": "StructStringVal"  }}
{  }`,
				sqlColumn: "struct_field.nested_struct.nested_struct_string_field",
			},
			wantQuery: `select * from(select
	case
		when "StructField" is null then null
		else struct_pack(
			"nested_struct" := case
				when "StructField"."NestedStruct" is null then null
				else struct_pack(
					"nested_struct_string_field" := "StructField"."NestedStruct"."NestedStructStringField"::varchar
				)
			end,
			"struct_string_field" := "StructField"."StructStringField"::varchar
		)
	end as "struct_field"
from
	read_ndjson(
		'%s',
		columns = {
			"StructField": 'struct("NestedStruct" struct("NestedStructStringField" varchar), "StructStringField" varchar)'
		}
	))`,
			wantData: []any{"NestedStructStringVal", nil},
		},
		{
			name: "nested struct with keyword names",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
						Columns: []*schema.ColumnSchema{
							{
								SourceName: "end",
								ColumnName: "end",
								Type:       "STRUCT",
								StructFields: []*schema.ColumnSchema{
									{
										SourceName: "any",
										ColumnName: "any",
										Type:       "STRUCT",
										StructFields: []*schema.ColumnSchema{
											{
												SourceName: "for",
												ColumnName: "for",
												Type:       "VARCHAR",
											},
										},
									},
								},
							},
						},
					},
				},
				json:      `{  "end": {    "any": {      "for": "NestedStructStringVal"    }}}`,
				sqlColumn: `"end"."any"."for"`,
			},
			wantQuery: `select * from(select
	case
		when "end" is null then null
		else struct_pack(
			"any" := case
				when "end"."any" is null then null
				else struct_pack(
					"for" := "end"."any"."for"::varchar
				)
			end
		)
	end as "end"
from
	read_ndjson(
		'%s',
		columns = {
			"end": 'struct("any" struct("for" varchar))'
		}
	))`,
			wantData: []any{"NestedStructStringVal"},
		},
		{
			name: "scalar types",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{"BooleanField": true, "TinyIntField": 1, "SmallIntField": 2, "IntegerField": 3, "BigIntField": 4, "UTinyIntField": 5, "USmallIntField": 6, "UIntegerField": 7, "UBigIntField": 8, "FloatField": 1.23, "DoubleField": 4.56, "VarcharField": "StringValue", "TimestampField": "2024-01-01T00:00:00Z"}`,
				sqlColumn: "varchar_field",
			},
			wantQuery: `select * from(select
	"BooleanField" as "boolean_field",
	"TinyIntField" as "tinyint_field",
	"SmallIntField" as "smallint_field",
	"IntegerField" as "integer_field",
	"BigIntField" as "bigint_field",
	"UTinyIntField" as "utinyint_field",
	"USmallIntField" as "usmallint_field",
	"UIntegerField" as "uinteger_field",
	"UBigIntField" as "ubigint_field",
	"FloatField" as "float_field",
	"DoubleField" as "double_field",
	"VarcharField" as "varchar_field",
	"TimestampField" as "timestamp_field"
from
	read_ndjson(
		'%s',
		columns = {
			"BooleanField": 'boolean', 
			"TinyIntField": 'tinyint', 
			"SmallIntField": 'smallint', 
			"IntegerField": 'integer', 
			"BigIntField": 'bigint', 
			"UTinyIntField": 'utinyint', 
			"USmallIntField": 'usmallint', 
			"UIntegerField": 'uinteger', 
			"UBigIntField": 'ubigint', 
			"FloatField": 'float', 
			"DoubleField": 'double', 
			"VarcharField": 'varchar', 
			"TimestampField": 'timestamp'
		}
	))`,
			wantData: []any{"StringValue"},
		},
		{
			name: "scalar types - reserved names",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
						Columns: []*schema.ColumnSchema{
							{SourceName: "end", ColumnName: "end", Type: "BOOLEAN"},
							{SourceName: "for", ColumnName: "for", Type: "TINYINT"},
						},
					},
				},
				json:      `{"end": true, "for": 1}`,
				sqlColumn: `"end"`,
			},
			wantQuery: `select * from(select
	"end" as "end",
	"for" as "for"
from
	read_ndjson(
		'%s',
		columns = {
			"end": 'boolean', 
			"for": 'tinyint'
		}
	))`,
			wantData: []any{true},
		},
		{
			name: "scalar types - missing some data",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{"BooleanField": true}`,
				sqlColumn: "boolean_field",
			},
			wantQuery: `select * from(select
	"BooleanField" as "boolean_field",
	"TinyIntField" as "tinyint_field",
	"SmallIntField" as "smallint_field",
	"IntegerField" as "integer_field",
	"BigIntField" as "bigint_field",
	"UTinyIntField" as "utinyint_field",
	"USmallIntField" as "usmallint_field",
	"UIntegerField" as "uinteger_field",
	"UBigIntField" as "ubigint_field",
	"FloatField" as "float_field",
	"DoubleField" as "double_field",
	"VarcharField" as "varchar_field",
	"TimestampField" as "timestamp_field"
from
	read_ndjson(
		'%s',
		columns = {
			"BooleanField": 'boolean', 
			"TinyIntField": 'tinyint', 
			"SmallIntField": 'smallint', 
			"IntegerField": 'integer', 
			"BigIntField": 'bigint', 
			"UTinyIntField": 'utinyint', 
			"USmallIntField": 'usmallint', 
			"UIntegerField": 'uinteger', 
			"UBigIntField": 'ubigint', 
			"FloatField": 'float', 
			"DoubleField": 'double', 
			"VarcharField": 'varchar', 
			"TimestampField": 'timestamp'
		}
	))`,
			wantData: []any{true},
		},
		{
			name: "scalar types - some rows missing some data",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json: `{"BooleanField": true}
{"TinyIntField": 1}
{"TinyIntField": 1, "BooleanField": true}`,
				sqlColumn: "boolean_field",
			},
			wantQuery: `select * from(select
	"BooleanField" as "boolean_field",
	"TinyIntField" as "tinyint_field",
	"SmallIntField" as "smallint_field",
	"IntegerField" as "integer_field",
	"BigIntField" as "bigint_field",
	"UTinyIntField" as "utinyint_field",
	"USmallIntField" as "usmallint_field",
	"UIntegerField" as "uinteger_field",
	"UBigIntField" as "ubigint_field",
	"FloatField" as "float_field",
	"DoubleField" as "double_field",
	"VarcharField" as "varchar_field",
	"TimestampField" as "timestamp_field"
from
	read_ndjson(
		'%s',
		columns = {
			"BooleanField": 'boolean', 
			"TinyIntField": 'tinyint', 
			"SmallIntField": 'smallint', 
			"IntegerField": 'integer', 
			"BigIntField": 'bigint', 
			"UTinyIntField": 'utinyint', 
			"USmallIntField": 'usmallint', 
			"UIntegerField": 'uinteger', 
			"UBigIntField": 'ubigint', 
			"FloatField": 'float', 
			"DoubleField": 'double', 
			"VarcharField": 'varchar', 
			"TimestampField": 'timestamp'
		}
	))`,
			wantData: []any{true, nil, true},
		},
		{
			name: "scalar types, missing all data",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{}`,
				sqlColumn: "varchar_field",
			},
			wantQuery: `select * from(select
	"BooleanField" as "boolean_field",
	"TinyIntField" as "tinyint_field",
	"SmallIntField" as "smallint_field",
	"IntegerField" as "integer_field",
	"BigIntField" as "bigint_field",
	"UTinyIntField" as "utinyint_field",
	"USmallIntField" as "usmallint_field",
	"UIntegerField" as "uinteger_field",
	"UBigIntField" as "ubigint_field",
	"FloatField" as "float_field",
	"DoubleField" as "double_field",
	"VarcharField" as "varchar_field",
	"TimestampField" as "timestamp_field"
from
	read_ndjson(
		'%s',
		columns = {
			"BooleanField": 'boolean', 
			"TinyIntField": 'tinyint', 
			"SmallIntField": 'smallint', 
			"IntegerField": 'integer', 
			"BigIntField": 'bigint', 
			"UTinyIntField": 'utinyint', 
			"USmallIntField": 'usmallint', 
			"UIntegerField": 'uinteger', 
			"UBigIntField": 'ubigint', 
			"FloatField": 'float', 
			"DoubleField": 'double', 
			"VarcharField": 'varchar', 
			"TimestampField": 'timestamp'
		}
	))`,
			wantData: []any{nil},
		},
		{
			name: "array types",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{"BooleanArrayField": [true, false], "TinyIntArrayField": [1, 2], "SmallIntArrayField": [2, 3], "IntegerArrayField": [3, 4], "BigIntArrayField": [4, 5], "UTinyIntArrayField": [5, 6], "USmallIntArrayField": [6, 7], "UIntegerArrayField": [7, 8], "UBigIntArrayField": [8, 9], "FloatArrayField": [1.23, 2.34], "DoubleArrayField": [4.56, 5.67], "VarcharArrayField": ["StringValue1", "StringValue2"], "TimestampArrayField": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]}`,
				sqlColumn: "boolean_array_field",
			},
			wantQuery: `select * from(select
	"BooleanArrayField" as "boolean_array_field",
	"TinyIntArrayField" as "tinyint_array_field",
	"SmallIntArrayField" as "smallint_array_field",
	"IntegerArrayField" as "integer_array_field",
	"BigIntArrayField" as "bigint_array_field",
	"UTinyIntArrayField" as "utinyint_array_field",
	"USmallIntArrayField" as "usmallint_array_field",
	"UIntegerArrayField" as "uinteger_array_field",
	"UBigIntArrayField" as "ubigint_array_field",
	"FloatArrayField" as "float_array_field",
	"DoubleArrayField" as "double_array_field",
	"VarcharArrayField" as "varchar_array_field",
	"TimestampArrayField" as "timestamp_array_field"
from
	read_ndjson(
		'%s',
		columns = {
			"BooleanArrayField": 'boolean[]', 
			"TinyIntArrayField": 'tinyint[]', 
			"SmallIntArrayField": 'smallint[]', 
			"IntegerArrayField": 'integer[]', 
			"BigIntArrayField": 'bigint[]', 
			"UTinyIntArrayField": 'utinyint[]', 
			"USmallIntArrayField": 'usmallint[]', 
			"UIntegerArrayField": 'uinteger[]', 
			"UBigIntArrayField": 'ubigint[]', 
			"FloatArrayField": 'float[]', 
			"DoubleArrayField": 'double[]', 
			"VarcharArrayField": 'varchar[]', 
			"TimestampArrayField": 'timestamp[]'
		}
	))`,
			wantData: []any{[]any{true, false}},
		},
		{
			name: "array of simple structs",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}`,
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid`,
			wantData: []any{"StringValue1"},
		},

		// TODO struct arrays are not supported yet
		// in fact one level of struct array field does work, but not nested struct arrays so for
		// now all struct arrays are treated as JSON
		//		{
		//			name: "struct with struct array field",
		//			args: args{
		//				conversionSchema: &conversionSchema.TableSchema{
		//					Columns: []*conversionSchema.ColumnSchema{
		//						{
		//							SourceName: "StructWithArrayField",
		//							ColumnName: "struct_with_array_field",
		//							Type:       "STRUCT",
		//							StructFields: []*conversionSchema.ColumnSchema{
		//								{SourceName: "StructArrayField",
		//									ColumnName: "struct_array_field",
		//									Type:       "STRUCT[]",
		//									StructFields: []*conversionSchema.ColumnSchema{
		//										{SourceName: "StructStringField", ColumnName: "struct_string_field", Type: "VARCHAR"},
		//										{SourceName: "StructIntField", ColumnName: "struct_int_field", Type: "INTEGER"},
		//									},},
		//							},
		//						},
		//					},
		//				},
		//				json:      `{"StructWithArrayField": {"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}}`,
		//				sqlColumn: "struct_with_array_field.struct_array_field[1].struct_string_field",
		//			},
		//			wantQuery: `WITH raw AS (
		//	SELECT
		//		row_number() OVER () AS rowid,
		//		"StructArrayField" AS "struct_array_field"
		//	FROM
		//		read_ndjson(
		//			'%s',
		//			columns = {
		//				"StructArrayField": 'STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]'
		//			}
		//		)
		//), unnest_struct_array_field AS (
		//    SELECT
		//        rowid,
		//		UNNEST(COALESCE("struct_array_field", ARRAY[]::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[])::STRUCT("StructStringField" VARCHAR, "StructIntField" INTEGER)[]) AS struct_array_field
		//	FROM
		//		raw
		//), rebuild_unnest_struct_array_field AS (
		//	SELECT
		//		rowid,
		//		struct_array_field->>'StructStringField' AS StructArrayField_StructStringField,
		//		struct_array_field->>'StructIntField' AS StructArrayField_StructIntField
		//	FROM
		//		unnest_struct_array_field
		//), grouped_unnest_struct_array_field AS (
		//	SELECT
		//		rowid,
		//		array_agg(struct_pack(
		//				struct_string_field := StructArrayField_StructStringField::VARCHAR,
		//				struct_int_field := StructArrayField_StructIntField::INTEGER
		//		)) AS struct_array_field
		//	FROM
		//		rebuild_unnest_struct_array_field
		//	GROUP BY
		//		rowid
		//)
		//SELECT
		//	COALESCE(joined_struct_array_field.struct_array_field, NULL) AS struct_array_field
		//FROM
		//	raw
		//LEFT JOIN
		//	grouped_unnest_struct_array_field joined_struct_array_field ON raw.rowid = joined_struct_array_field.rowid`,
		//			wantData: []any{"StringValue1"},
		//		},

		{
			name: "array of simple structs plus other fields",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},

				json: `{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}], "IntField": 10, "StringField": "SampleString", "FloatField": 10.5, "BooleanField": true, "IntArrayField": [1, 2, 3], "StringArrayField": ["String1", "String2"], "FloatArrayField": [1.1, 2.2, 3.3], "BooleanArrayField": [true, false, true]}`,
				// NOTE: arrays are 1-based
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field",
		"IntField" as "int_field",
		"StringField" as "string_field",
		"FloatField" as "float_field",
		"BooleanField" as "boolean_field",
		"IntArrayField" as "int_array_field",
		"StringArrayField" as "string_array_field",
		"FloatArrayField" as "float_array_field",
		"BooleanArrayField" as "boolean_array_field"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]', 
				"IntField": 'integer', 
				"StringField": 'varchar', 
				"FloatField": 'float', 
				"BooleanField": 'boolean', 
				"IntArrayField": 'integer[]', 
				"StringArrayField": 'varchar[]', 
				"FloatArrayField": 'float[]', 
				"BooleanArrayField": 'boolean[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field,
	raw.int_field,
	raw.string_field,
	raw.float_field,
	raw.boolean_field,
	raw.int_array_field,
	raw.string_array_field,
	raw.float_array_field,
	raw.boolean_array_field
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid`,
			wantData: []any{"StringValue1"},
		},
		{
			name: "null array of simple structs plus other fields",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},

				json:      `{"StructArrayField": null, "IntField": 10, "StringField": "SampleString", "FloatField": 10.5, "BooleanField": true, "IntArrayField": [1, 2, 3], "StringArrayField": ["String1", "String2"], "FloatArrayField": [1.1, 2.2, 3.3], "BooleanArrayField": [true, false, true]}`,
				sqlColumn: "int_field",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field",
		"IntField" as "int_field",
		"StringField" as "string_field",
		"FloatField" as "float_field",
		"BooleanField" as "boolean_field",
		"IntArrayField" as "int_array_field",
		"StringArrayField" as "string_array_field",
		"FloatArrayField" as "float_array_field",
		"BooleanArrayField" as "boolean_array_field"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]', 
				"IntField": 'integer', 
				"StringField": 'varchar', 
				"FloatField": 'float', 
				"BooleanField": 'boolean', 
				"IntArrayField": 'integer[]', 
				"StringArrayField": 'varchar[]', 
				"FloatArrayField": 'float[]', 
				"BooleanArrayField": 'boolean[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field,
	raw.int_field,
	raw.string_field,
	raw.float_field,
	raw.boolean_field,
	raw.int_array_field,
	raw.string_array_field,
	raw.float_array_field,
	raw.boolean_array_field
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid`,
			wantData: []any{int32(10)},
		},
		{
			name: "array of simple structs with null value",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json:      `{"StructArrayField": null}`,
				sqlColumn: "struct_array_field",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid`,
			wantData: []any{nil},
		},
		{
			name: "array of simple structs with null value and non null value",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
				},
				json: `{"StructArrayField": null}
{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}]}`,
				sqlColumn: "struct_array_field[1].struct_string_field",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid`,
			//wantData: []any{nil, "StringValue1"},
			// NOTE: ordering is not guaranteed
			wantData: []any{"StringValue1", nil},
		},
		{
			name: "2 arrays of simple structs",
			args: args{
				schema: &schema.ConversionSchema{
					TableSchema: schema.TableSchema{
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
							{
								SourceName: "StructArrayField2",
								ColumnName: "struct_array_field2",
								Type:       "STRUCT[]",
								StructFields: []*schema.ColumnSchema{
									{SourceName: "StructStringField2", ColumnName: "struct_string_field2", Type: "VARCHAR"},
									{SourceName: "StructIntField2", ColumnName: "struct_int_field2", Type: "INTEGER"},
								},
							},
						},
					},
				},
				json:      `{"StructArrayField": [{"StructStringField": "StringValue1", "StructIntField": 1}, {"StructStringField": "StringValue2", "StructIntField": 2}], "StructArrayField2": [{"StructStringField2": "StringValue100", "StructIntField2": 100}, {"StructStringField2": "StringValue200", "StructIntField2": 200}]}`,
				sqlColumn: "struct_array_field2[1].struct_string_field2",
			},
			wantQuery: `with raw as (
	select * from(select
		row_number() over () as rowid,
		"StructArrayField" as "struct_array_field",
		"StructArrayField2" as "struct_array_field2"
	from
		read_ndjson(
			'%s',
			columns = {
				"StructArrayField": 'struct("StructStringField" varchar, "StructIntField" integer)[]', 
				"StructArrayField2": 'struct("StructStringField2" varchar, "StructIntField2" integer)[]'
			}
		))
), unnest_struct_array_field as (
    select
        rowid,
		unnest(coalesce("struct_array_field", array[]::struct("StructStringField" varchar, "StructIntField" integer)[])::struct("StructStringField" varchar, "StructIntField" integer)[]) as struct_array_field
	from
		raw
), rebuild_unnest_struct_array_field as (
	select
		rowid,
		struct_array_field->>'StructStringField' as StructArrayField_StructStringField,
		struct_array_field->>'StructIntField' as StructArrayField_StructIntField
	from
		unnest_struct_array_field
), grouped_unnest_struct_array_field as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field := StructArrayField_StructStringField::varchar,
				struct_int_field := StructArrayField_StructIntField::integer
		)) as struct_array_field	
	from
		rebuild_unnest_struct_array_field	
	group by
		rowid	
), unnest_struct_array_field2 as (
    select
        rowid,
		unnest(coalesce("struct_array_field2", array[]::struct("StructStringField2" varchar, "StructIntField2" integer)[])::struct("StructStringField2" varchar, "StructIntField2" integer)[]) as struct_array_field2
	from
		raw
), rebuild_unnest_struct_array_field2 as (
	select
		rowid,
		struct_array_field2->>'StructStringField2' as StructArrayField2_StructStringField2,
		struct_array_field2->>'StructIntField2' as StructArrayField2_StructIntField2
	from
		unnest_struct_array_field2
), grouped_unnest_struct_array_field2 as (
	select
		rowid,	
		array_agg(struct_pack(
				struct_string_field2 := StructArrayField2_StructStringField2::varchar,
				struct_int_field2 := StructArrayField2_StructIntField2::integer
		)) as struct_array_field2	
	from
		rebuild_unnest_struct_array_field2	
	group by
		rowid	
)
select
	coalesce(joined_struct_array_field.struct_array_field, null) as struct_array_field,
	coalesce(joined_struct_array_field2.struct_array_field2, null) as struct_array_field2
from
	raw	
left join
	grouped_unnest_struct_array_field joined_struct_array_field on raw.rowid = joined_struct_array_field.rowid
left join
	grouped_unnest_struct_array_field2 joined_struct_array_field2 on raw.rowid = joined_struct_array_field2.rowid`,
			wantData: []any{"StringValue100"},
		},
		// TODO #parquet https://github.com/turbot/tailpipe/issues/new
		//		{
		//			name: "map types",
		//			args: args{
		//				conversionSchema: &conversionSchema.TableSchema{
		//					Columns: []*conversionSchema.ColumnSchema{
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
		//			wantQuery: `SELECT
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
			conversionSchema := schema.NewConversionSchema(&tt.args.schema.TableSchema)
			query := buildViewQuery(conversionSchema)

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

func executeQuery(t *testing.T, queryFormat, json, sqlColumn string) (any, error) {

	// now verify the query runs
	// copy json to a jsonl file
	err := createJSONLFile(json)
	if err != nil {
		t.Fatalf("error creating jsonl file: %s", err)
	}
	defer os.Remove(jsonlFilePath)

	// render query with the file path
	query := fmt.Sprintf(queryFormat, jsonlFilePath)

	// get the data
	var data []any

	// execute in duckdb
	// build select queryz
	testQuery := fmt.Sprintf("select %s from (%s)", sqlColumn, query)
	rows, err := testDb.Query(testQuery)

	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	// Iterate over the results
	for rows.Next() {
		var d any

		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("error scanning data: %w", err)
		}
		data = append(data, d)
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
