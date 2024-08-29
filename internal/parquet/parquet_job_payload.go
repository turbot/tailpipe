package parquet

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"time"
)

type JobPayload struct {
	PartitionName        string
	Schema               *schema.RowSchema
	UpdateActiveDuration func(duration time.Duration)
}
