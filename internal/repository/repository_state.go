package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"

	"github.com/turbot/tailpipe/internal/filepaths"
)

// Default timeout for acquiring file locks
const defaultLockTimeout = 10 * time.Second

// PartitionState represents the state of a partition collection
type PartitionState string

const (
	CollectionStateIdle         PartitionState = "idle"
	CollectionStateInCollecting PartitionState = "collecting"
	CollectionStateInvalid      PartitionState = "invalid"
)

// PartitionStateInfo represents the repository state information for a partition
type PartitionStateInfo struct {
	State PartitionState `json:"state"`
	// The date from which the partition is considered invalid - should be set if state is invalid
	InvalidFromDate time.Time `json:"invalid_from_date,omitempty"`
	// The PID of the process that is currently collecting the partition
	// should be set when State is in-progress
	PID int `json:"pid,omitempty"`
}

func (i PartitionStateInfo) PreviousCollectionAborted() bool {
	if i.State != CollectionStateInCollecting {
		return false
	}

	if i.PID == 0 {
		slog.Info("PartitionStateInfo.PreviousCollectionAborted state is in collecting but PID is 0 - assume aborted")
		// unexpected - we should have a PID
		return true
	}

	// check if the process is running
	p, err := utils.FindProcess(i.PID)
	if err != nil || p == nil {
		slog.Info("PartitionStateInfo.PreviousCollectionAborted can't find process, assume aborted", "pid", i.PID)
		// if we cannot find the process, assume it is not running
		return true
	}
	running, err := p.IsRunning()
	if err != nil {
		slog.Info("PartitionStateInfo.PreviousCollectionAborted can't determine if process is running, assume aborted", "pid", i.PID)
		// if we cannot determine if the process is running, assume it is not
		return true
	}
	// if the process is running we are note aborted
	slog.Info("PartitionStateInfo.PreviousCollectionAborted", "pid", i.PID, "process running", running, "aborted", !running)
	return !running
}

// SetIdle sets the partition state to idle and clears the invalid from date
func (i *PartitionStateInfo) SetIdle() {
	slog.Info("SetIdle - setting partition state to idle", "partition", i)
	i.InvalidFromDate = time.Time{}
	i.State = CollectionStateIdle
	i.PID = 0
}

// RepositoryState represents the state of the local repository
type RepositoryState struct {
	// map of partition name to state info
	Partitions map[string]PartitionStateInfo `json:"partitions"`
	statePath  string
}

// Load loads the repository state from disk using the default state path
func Load() (*RepositoryState, error) {
	rs := &RepositoryState{
		Partitions: make(map[string]PartitionStateInfo),
		statePath:  filepaths.GetLocalRepositoryStatePath(),
	}

	if err := rs.Load(); err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	return rs, nil
}

// GetPartitionState gets the state for a partition from memory
func (rs *RepositoryState) GetPartitionState(partitionFullName string) PartitionStateInfo {
	return rs.Partitions[partitionFullName]
}

// SetPartitionState sets the state for a partition in memory and saves it to disk
func (rs *RepositoryState) SetPartitionState(partitionFullName string, state PartitionStateInfo) {
	rs.Partitions[partitionFullName] = state

}

// SetPartitionCollectionState sets the collection state and PID for a partition and saves it to disk
func (rs *RepositoryState) SetPartitionCollectionState(partition string, collectionState PartitionState, pid int) {
	info := rs.Partitions[partition]
	info.State = collectionState
	info.PID = pid
	rs.Partitions[partition] = info
}

// SetPartitionInvalidFromDate sets the invalid from date for a partition and saves it to disk
func (rs *RepositoryState) SetPartitionInvalidFromDate(partitionFullName string, invalidFromDate time.Time) {
	partitionState := rs.GetPartitionState(partitionFullName)
	partitionState.InvalidFromDate = invalidFromDate
	rs.SetPartitionState(partitionFullName, partitionState)
}

// Save persists the current state to disk
func (rs *RepositoryState) Save() error {
	return rs.withFileLock(func() error {
		data, err := json.MarshalIndent(rs, "", "    ")
		if err != nil {
			return fmt.Errorf("failed to marshal state: %w", err)
		}

		if err := os.WriteFile(rs.statePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write state file: %w", err)
		}

		return nil
	})
}

// Load loads the repository state from disk
func (rs *RepositoryState) Load() error {
	return rs.withFileLock(func() error {
		data, err := os.ReadFile(rs.statePath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil // Return empty state for new files
			}
			return fmt.Errorf("failed to read state file: %w", err)
		}

		if len(data) == 0 {
			return nil // Return empty state for empty files
		}

		if err := json.Unmarshal(data, rs); err != nil {
			return fmt.Errorf("failed to unmarshal state: %w", err)
		}

		return nil
	})
}

// withFileLock executes fn while holding an exclusive lock on the state file
func (rs *RepositoryState) withFileLock(fn func() error) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultLockTimeout)
	defer cancel()

	// open state file with exclusive lock
	file, err := os.OpenFile(rs.statePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	// try to acquire exclusive lock with timeout
	lockChan := make(chan error, 1)
	go func() {
		lockChan <- syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	}()

	select {
	case err := <-lockChan:
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for lock (another process may be using the repository)")
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

	return fn()
}
