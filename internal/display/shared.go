package display

import (
	"github.com/dustin/go-humanize"
	"math"
)

func humanizeBytes(bytes int64) string {
	if bytes == 0 {
		return "-"
	}
	return humanize.Bytes(uint64(math.Max(float64(bytes), 0)))
}

func humanizeCount(count int64) string {
	if count == 0 {
		return "-"
	}
	return humanize.Comma(count)
}
