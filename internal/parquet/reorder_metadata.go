package parquet

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe/internal/database"
	"time"
)

type reorderMetadata struct {
	pk              *partitionKey
	unorderedRanges []unorderedDataTimeRange

	rowCount     int64
	maxRowId     int64
	minTimestamp time.Time
	maxTimestamp time.Time
}

func newReorderMetadata(ctx context.Context, db *database.DuckDb, p *partitionKey) (*reorderMetadata, error) {
	var rm = &reorderMetadata{pk: p}

	// Query to get row count and time range for this partition
	countQuery := fmt.Sprintf(`select count(*), max(rowid), min(tp_timestamp), max(tp_timestamp) from "%s" 
		where tp_partition = ?
		  and tp_index = ?
		  and year(tp_timestamp) = ?
		  and month(tp_timestamp) = ?`,
		p.tpTable)

	err := db.QueryRowContext(ctx, countQuery,
		p.tpPartition,
		p.tpIndex,
		p.year,
		p.month).Scan(&rm.rowCount, &rm.maxRowId, &rm.minTimestamp, &rm.maxTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count and time range for partition: %w", err)
	}

	return rm, nil
}
