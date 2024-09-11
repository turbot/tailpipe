package parquet

import (
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
	"time"
)

type JobPayload struct {
	Partition            *config.Partition
	Schema               *schema.RowSchema
	UpdateActiveDuration func(duration time.Duration)
}
