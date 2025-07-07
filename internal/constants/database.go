package constants

import "time"

const (
	TailpipeDbName = "tailpipe.db"
	DbFileMaxAge   = 24 * time.Hour
	DuckLakeSchema = "tailpipe_ducklake"
)
