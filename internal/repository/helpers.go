package repository

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// CheckPartitionState verifies if a partition is in a valid state.
// It performs two main checks:
// 1. Verifies the partition is not currently being collected (CollectionStateInProgress)
// 2. Checks if there are any invalid parquet files in the partition (InvalidFromDate)
//
// Returns:
// - An error if the partition cannot be collected (e.g., already being collected)
// - A warning if there are invalid parquet files that may affect data completeness
// - Empty ErrorAndWarnings if the partition is in a valid state for collection
func CheckPartitionState(partition *config.Partition) error_helpers.ErrorAndWarnings {
	rs, err := loadRepositoryState()
	if err != nil {
		return error_helpers.NewErrorsAndWarning(err)
	}

	partitionState, err := GetPartitionState(partition.ShortName)
	if err != nil {
		return error_helpers.NewErrorsAndWarning(fmt.Errorf("failed to check partition state: %w", err))
	}

	// If state is in progress but process doesn't exist, mark as invalid
	if partitionState.CollectionState == CollectionStateInProgress && (partitionState.PID == 0 || !utils.PidExists(partitionState.PID)) {

		dataDir := config.GlobalWorkspaceProfile.GetDataDir()
		// search for earliest dated temp files in the collection temp dir and set the invalidfrom date to that
		invalidFromDate := findEarliestTempFile(partition, dataDir).Add(-1 * time.Hour * 24)
		partitionState.InvalidFromDate = invalidFromDate
		partitionState.CollectionState = CollectionStateInvalid
		rs.SetPartitionState(partition.UnqualifiedName, partitionState)
		// save the repo state
		if err := rs.Save(); err != nil {
			return error_helpers.NewErrorsAndWarning(err)
		}
	}

	if partitionState.CollectionState == CollectionStateInProgress {
		return error_helpers.NewErrorsAndWarning(fmt.Errorf("partition %s is already being collected", partition.UnqualifiedName))
	}

	if !partitionState.InvalidFromDate.IsZero() {
		return error_helpers.NewErrorsAndWarning(fmt.Errorf("partition %s has invalid data from %s",
			partition.ShortName,
			partitionState.InvalidFromDate.Format("2006-01-02")))
	}

	return error_helpers.ErrorAndWarnings{}
}

// GetPartitionState gets the state for a partition from the state file.
// If it discovers a stale collection (in-progress but process not running),
// it will update the state to invalid and save the change.
func GetPartitionState(partitionName string) (PartitionStateInfo, error) {
	rs, err := loadRepositoryState()
	if err != nil {
		return PartitionStateInfo{}, err
	}
	return rs.GetPartitionState(partitionName), nil
}

// UpdatePartitionState updates the state for a partition in the state file.
func UpdatePartitionState(partition string, collectionState CollectionState, invalidFromDate time.Time, pid int) error {
	rs, err := loadRepositoryState()
	if err != nil {
		return err
	}

	partitionState := rs.GetPartitionState(partition)
	partitionState.CollectionState = collectionState
	partitionState.PID = pid
	partitionState.InvalidFromDate = invalidFromDate
	rs.SetPartitionState(partition, partitionState)

	return rs.Save()
}

func findEarliestTempFile(partition *config.Partition, dataDir string) time.Time {

	pattern := filepaths.GetTempParquetFileGlobForPartition(dataDir, partition.TableName, partition.ShortName, "")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return time.Now()
	}

	earliest := time.Now()
	for _, match := range matches {
		partitionFields, err := filepaths.ExtractPartitionFields(match)
		if err != nil {
			// just ignore files we can't parse
			continue
		}
		if !partitionFields.Date.IsZero() && partitionFields.Date.Before(earliest) {
			// if date was parsed and is before the earliest date, update the earliest date
			earliest = partitionFields.Date
		}
	}
	return earliest
}
