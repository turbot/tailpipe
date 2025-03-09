package repository

import (
	"fmt"
	"time"
)

// HandlePartitionErrors updates the state for all invalid partitions reported in the error.
// For each partition with invalid data, it:
// 1. Sets the collection state to idle
// 2. Records the earliest error date as the invalid from date
func HandlePartitionErrors(partitionErrors map[string]time.Time) error {
	if partitionErrors == nil {
		return nil
	}

	rs, err := Load()
	if err != nil {
		return fmt.Errorf("failed to load repository state: %w", err)
	}

	// Update state for each invalid partition
	for partition, errTime := range partitionErrors {
		partitionState := rs.GetPartitionState(partition)
		partitionState.InvalidFromDate = errTime
		partitionState.State = CollectionStateIdle
		rs.SetPartitionState(partition, partitionState)
	}

	// Save the updated state
	if err := rs.Save(); err != nil {
		return fmt.Errorf("failed to save repository state: %w", err)
	}

	return nil
}
