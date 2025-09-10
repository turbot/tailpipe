package database

import (
	"fmt"
	"strings"

	"github.com/danwakefield/fnmatch"
	"github.com/turbot/tailpipe/internal/config"
	"golang.org/x/exp/maps"
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

// PartitionMatchesPatterns checks if the given table and partition match any of the provided patterns.
func PartitionMatchesPatterns(table, partition string, patterns []*PartitionPattern) bool {
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

// GetPartitionsForArg returns the actual partition names that match the given argument.
// The partitionNames list is needed to determine whether a single-part argument refers to a table or partition.
func GetPartitionsForArg(partitionMap map[string]*config.Partition, arg string) ([]string, error) {
	partitionNames := maps.Keys(partitionMap)
	partitionPattern, err := GetPartitionPatternsForArgs(partitionNames, arg)
	if err != nil {
		return nil, err
	}
	// now match the partition
	var res []string
	for _, partition := range partitionMap {
		if PartitionMatchesPatterns(partition.TableName, partition.ShortName, partitionPattern) {
			res = append(res, partition.UnqualifiedName)
		}
	}
	return res, nil
}

// GetPartitionPatternsForArgs returns the table and partition patterns for the given partition args.
// The partitions list is needed to determine whether single-part arguments refer to tables or partitions.
func GetPartitionPatternsForArgs(partitions []string, partitionArgs ...string) ([]*PartitionPattern, error) {
	var res []*PartitionPattern
	for _, arg := range partitionArgs {
		partitionPattern, err := GetPartitionMatchPatternsForArg(partitions, arg)
		if err != nil {
			return nil, fmt.Errorf("error processing partition arg '%s': %w", arg, err)
		}

		res = append(res, partitionPattern)
	}

	return res, nil
}

// GetPartitionMatchPatternsForArg parses a single partition argument into a PartitionPattern.
// The partitions list is needed to determine whether single-part arguments refer to tables or partitions.
func GetPartitionMatchPatternsForArg(partitions []string, arg string) (*PartitionPattern, error) {
	var partitionPattern *PartitionPattern
	parts := strings.Split(arg, ".")
	switch len(parts) {
	case 1:
		var err error
		partitionPattern, err = getPartitionPatternsForSinglePartName(partitions, arg)
		if err != nil {
			return nil, err
		}
	case 2:
		// use the args as provided
		partitionPattern = &PartitionPattern{Table: parts[0], Partition: parts[1]}
	default:
		return nil, fmt.Errorf("invalid partition name: %s", arg)
	}
	return partitionPattern, nil
}

// getPartitionPatternsForSinglePartName determines whether a single-part argument refers to a table or partition.
// The partitions list is needed to check if the argument matches any existing table names.
// e.g. if the arg is "aws*" and it matches table "aws_cloudtrail_log", it's treated as a table pattern.
func getPartitionPatternsForSinglePartName(partitions []string, arg string) (*PartitionPattern, error) {
	var tablePattern, partitionPattern string
	// '*' is not valid for a single part arg
	if arg == "*" {
		return nil, fmt.Errorf("invalid partition name: %s", arg)
	}
	// check whether there is table with this name
	// partitions is a list of Unqualified names, i.e. <table>.<partition>
	for _, partition := range partitions {
		table := strings.Split(partition, ".")[0]

		// if the arg matches a table name, set table pattern to the arg and partition pattern to *
		if fnmatch.Match(arg, table, fnmatch.FNM_CASEFOLD) {
			tablePattern = arg
			partitionPattern = "*"
			return &PartitionPattern{Table: tablePattern, Partition: partitionPattern}, nil
		}
	}
	// so there IS NOT a table with this name - set table pattern to * and user provided partition name
	tablePattern = "*"
	partitionPattern = arg
	return &PartitionPattern{Table: tablePattern, Partition: partitionPattern}, nil
}
