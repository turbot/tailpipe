package queryresult

import "github.com/turbot/pipe-fittings/queryresult"

type ResultStreamer = queryresult.ResultStreamer[*TimingMetadata]

func NewResultStreamer() *ResultStreamer {
	return queryresult.NewResultStreamer[*TimingMetadata]()
}
