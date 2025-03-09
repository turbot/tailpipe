package repository

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// SetPartitionStateCollecting sets the partition state to collecting
//
// It first verifies if a partition is in a valid state.
// It performs two main checks:
// 1. Verifies the partition is not currently being collected (CollectionStateInCollecting)
// 2. Checks if there are any invalid parquet files in the partition (InvalidFromDate)
//
// Returns:
// - An error if the partition cannot be collected (e.g., already being collected)
// - An error if there are invalid parquet files that may affect data completeness
// - nil if the partition is in a valid state for collection
func SetPartitionStateCollecting(partition *config.Partition, fromTime time.Time) error {
	// load the repository state
	rs, err := Load()
	if err != nil {
		slog.Warn("Failed to load repository state", "error", err)
		return err
	}

	// verify we can collect this partition
	partitionState := rs.GetPartitionState(partition.UnqualifiedName)

	// If state is in progress but process doesn't exist, mark as invalid
	if partitionState.PreviousCollectionAborted() {
		// as we pass partitionState by value, we need to get the state again
		partitionState, err = handleInterruptedCollection(partition, partitionState, rs)
		if err != nil {
			return err
		}
	}

	if partitionState.State == CollectionStateInCollecting {
		slog.Info("SetPartitionStateCollecting - partition is already being collected", "partition", partition.UnqualifiedName)
		// we cannot collect this partition because it is already being collected
		return fmt.Errorf("partition %s is already being collected by PID %d", partition.UnqualifiedName, partitionState.PID)
	}

	if partitionState.State == CollectionStateInvalid {
		slog.Info("SetPartitionStateCollecting - partition is in invalid state", "partition", partition.UnqualifiedName)
		// if we are NOT collecting from before the invalid from date, we cannot collect
		if partitionState.InvalidFromDate.IsZero() || fromTime.IsZero() || partitionState.InvalidFromDate.Before(fromTime) {
			return buildInvalidPartitionError(partition, partitionState.InvalidFromDate)
		}
	}

	slog.Info("SetPartitionStateCollecting - setting partition state to collecting", "partition", partition.UnqualifiedName)

	// Set state to collecting and save
	partitionState.State = CollectionStateInCollecting
	partitionState.PID = os.Getpid()

	rs.SetPartitionState(partition.UnqualifiedName, partitionState)
	return rs.Save()
}

func handleInterruptedCollection(partition *config.Partition, partitionState PartitionStateInfo, rs *RepositoryState) (PartitionStateInfo, error) {
	slog.Info("handleInterruptedCollection - setting partition state to invalid", "partition", partition.UnqualifiedName)
	// search for earliest dated temp files in the collection temp dir and set the invalidfrom date to that
	dataDir := config.GlobalWorkspaceProfile.GetDataDir()
	invalidFromDate := findEarliestTempFile(partition, dataDir)
	partitionState.InvalidFromDate = invalidFromDate
	partitionState.State = CollectionStateInvalid
	rs.SetPartitionState(partition.UnqualifiedName, partitionState)
	// save the repo state
	if err := rs.Save(); err != nil {
		slog.Warn("failed to save repository state", "error", err)
		return PartitionStateInfo{}, fmt.Errorf("failed to save repository state: %w", err)
	}
	return partitionState, nil
}

func SetPartitionStateComplete(partition *config.Partition, collectionErr error, fromDate time.Time) error {
	// load the repository state
	rs, err := Load()
	if err != nil {
		return err
	}

	partitionState := rs.GetPartitionState(partition.UnqualifiedName)

	// If state is already invalid we must have set it during collection upon an error - we have nothing to do
	if partitionState.State == CollectionStateInvalid {
		return nil
	}

	// we expect the state to be in progress
	if partitionState.State != CollectionStateInCollecting {
		return fmt.Errorf("partition %s is not currently being collected", partition.UnqualifiedName)
	}

	if collectionErr != nil {
		// set the state to invalid with the coLlection from date as the invalid from date
		partitionState.InvalidFromDate = fromDate
		partitionState.State = CollectionStateInvalid
		// clear PID
		partitionState.PID = 0

	} else {
		partitionState.InvalidFromDate = time.Time{}
		partitionState.State = CollectionStateIdle
		// clear PID
		partitionState.PID = 0
	}
	rs.SetPartitionState(partition.UnqualifiedName, partitionState)
	return rs.Save()
}

func buildInvalidPartitionError(partition *config.Partition, date time.Time) error {
	// if the invalid from date is not set - we assume the whole partition is invalid so tell the user to delete it
	if date.IsZero() {
		deleteCmd := fmt.Sprintf("tailpipe partition delete %s", partition.UnqualifiedName)
		return fmt.Errorf("partition %s has invalid data\ndelete the local partition data with the command:\n\t* %s", partition.UnqualifiedName, deleteCmd)
	}

	// otherwise give the user the option to delete the invalid data or collect from that date
	formattedDate := date.Format("2006-01-02")
	deleteFromCmd := fmt.Sprintf("tailpipe partition delete %s --from %s", partition.UnqualifiedName, formattedDate)
	collectFromCmd := fmt.Sprintf("tailpipe collect %s --from %s", partition.UnqualifiedName, formattedDate)
	return fmt.Errorf("partition %s has invalid data from %s onwards.\nRun either of: \n\t* %s\n\t* %s",
		partition.UnqualifiedName,
		formattedDate,
		deleteFromCmd,
		collectFromCmd)
}

// LoadPartitionState loads the repo state and gets the state for a partition from the state file.
func LoadPartitionState(partitionName string) (PartitionStateInfo, error) {
	rs, err := Load()
	if err != nil {
		return PartitionStateInfo{}, err
	}
	return rs.GetPartitionState(partitionName), nil
}

// UpdateAndSavePartitionState loads the repo state and updates the state for a partition in the state file, before saving the state
func UpdateAndSavePartitionState(partition string, collectionState PartitionState, invalidFromDate time.Time, pid int) error {
	rs, err := Load()
	if err != nil {
		return err
	}

	partitionState := rs.GetPartitionState(partition)
	partitionState.State = collectionState
	partitionState.PID = pid
	partitionState.InvalidFromDate = invalidFromDate
	rs.SetPartitionState(partition, partitionState)

	return rs.Save()
}

func findEarliestTempFile(partition *config.Partition, dataDir string) time.Time {
	pattern := filepaths.GetTempParquetFileGlobForPartition(dataDir, partition.TableName, partition.GetUnqualifiedName(), "")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return time.Time{}
	}

	earliest := time.Time{}
	for _, match := range matches {
		partitionFields, err := filepaths.ExtractPartitionFields(match)
		if err != nil {
			// just ignore files we can't parse
			continue
		}
		if !partitionFields.Date.IsZero() && (earliest.IsZero() || partitionFields.Date.Before(earliest)) {
			// if date was parsed and is before the earliest date, update the earliest date
			earliest = partitionFields.Date
		}
	}
	return earliest
}
