package parquet

type ColumnSchemaChange struct {
	Name    string
	OldType string
	NewType string
}

type SchemaChangeError struct {
	ChangedColumns []ColumnSchemaChange
}
