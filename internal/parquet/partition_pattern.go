package parquet

import (
	"github.com/danwakefield/fnmatch"
	"github.com/turbot/tailpipe/internal/config"
)

// PartitionPattern represents a pattern used to match partitions.
// It consists of a table pattern and a partition pattern, both of which are
// used to match a given table and partition name.
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

func PartitionMatchesPatterns(table, partition string, patterns []PartitionPattern) bool {
	if len(patterns) == 0 {
		return true
	}
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
