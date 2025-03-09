package database

// DuckDbOpt is a function type that modifies a DuckDb instance.
// It's used to configure DuckDb instances with different options
// like extensions, database file, and temp directory.
type DuckDbOpt func(*DuckDb)

// WithDuckDbExtensions sets the list of DuckDB extensions to be installed and loaded.
// These extensions will be installed and loaded when the DuckDb instance is created.
func WithDuckDbExtensions(extensions []string) DuckDbOpt {
	return func(d *DuckDb) {
		d.extensions = extensions
	}
}

// WithDbFile sets the database file path for the DuckDb instance.
// This can be used to specify a persistent database file or an in-memory database.
func WithDbFile(filename string) DuckDbOpt {
	return func(d *DuckDb) {
		d.dataSourceName = filename
	}
}

// WithTempDir sets the temporary directory for DuckDB operations.
// This directory is used for temporary files during database operations.
// If not specified, the collection temp directory will be used.
func WithTempDir(dir string) DuckDbOpt {
	return func(d *DuckDb) {
		d.tempDir = dir
	}
}
