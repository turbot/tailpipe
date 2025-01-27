package parquet

import (
	"github.com/danwakefield/fnmatch"
	"github.com/turbot/tailpipe/internal/config"
)

type PartitionPattern struct {
	Table     string
	Partition string
}

func NewPartitionPattern(partition *config.Partition) PartitionPattern {
	return PartitionPattern{
		Table:     partition.TableName,
		Partition: partition.ShortName,
	}
}

func ParquetPathMatchesPatterns(table, partition string, patterns []PartitionPattern) bool {
	// do ANY patterns match
	gotMatch := false
	for _, pattern := range patterns {
		if fnmatch.Match(pattern.Table, table, fnmatch.FNM_CASEFOLD) &&
			fnmatch.Match(pattern.Partition, partition, fnmatch.FNM_CASEFOLD) {
			gotMatch = true
		}
	}
	return gotMatch
}
