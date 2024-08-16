package parquet

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"time"
)

type ParquetJobPayload struct {
	// collection name
	CollectionName string
	// collection schema
	Schema *schema.RowSchema
	// function to update the active duration
	UpdateActiveDuration func(duration time.Duration)
}
