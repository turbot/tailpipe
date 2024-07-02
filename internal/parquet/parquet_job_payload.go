package parquet

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type ParquetJobPayload struct {
	// collection schema
	Schema *schema.RowSchema
}
