package constants

import "time"

const (
	DbFileMaxAge            = 24 * time.Hour
	DuckLakeCatalog         = "tailpipe_ducklake"
	DuckLakeMetadataCatalog = "__ducklake_metadata_" + DuckLakeCatalog
)
