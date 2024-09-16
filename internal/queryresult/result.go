package queryresult

import (
	"github.com/turbot/pipe-fittings/queryresult"
	"time"
)

type TimingMetadata struct {
	Duration time.Duration
}

type Result = queryresult.Result[*TimingMetadata]

func NewResult(cols []*queryresult.ColumnDef) *Result {
	return queryresult.NewResult[*TimingMetadata](cols, &TimingMetadata{})
}

type SyncQueryResult = queryresult.SyncQueryResult[*TimingMetadata]
