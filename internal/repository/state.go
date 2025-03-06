package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// Default timeout for acquiring file locks
const defaultLockTimeout = 10 * time.Second

// GetPartitionState gets the state for a partition from the state file.
// If it discovers a stale collection (in-progress but process not running),
// it will update the state to invalid and save the change.
func GetPartitionState(partition string) (*PartitionStateInfo, error) {
	rs, err := loadRepositoryState()
	if err != nil {
		return nil, err
	}

	info := rs.GetPartitionState(partition)
	if info == nil {
		return &PartitionStateInfo{}, nil
	}

	// If state is in progress but process doesn't exist, mark as invalid
	if info.State == PartitionStateInProgress && (info.PID == 0 || !utils.PidExists(info.PID)) {
		// Update state to invalid
		info.State = PartitionStateInvalid
		info.Message = "Collection was aborted - process no longer running"
		if err := rs.Save(); err != nil {
			return nil, fmt.Errorf("failed to update stale state: %w", err)
		}
		return info, nil
	}

	return info, nil
}

// UpdatePartitionState updates the state for a partition in the state file.
func UpdatePartitionState(partition string, state PartitionState, lastDay time.Time, message string, pid int) error {
	rs, err := loadRepositoryState()
	if err != nil {
		return err
	}

	// Update state
	rs.SetPartitionState(partition, state, lastDay, message, pid)
	if err := rs.Save(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	return nil
}

// loadRepositoryState loads the repository state from disk using the default state path
func loadRepositoryState() (*RepositoryState, error) {
	rs := &RepositoryState{
		Partitions: make(map[string]*PartitionStateInfo),
		statePath:  filepaths.GetLocalRepositoryStatePath(),
	}

	if err := rs.Load(); err != nil {
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	return rs, nil
}

// PartitionState represents the state of a partition
type PartitionState string

const (
	// PartitionStateOK indicates the partition is in a valid state
	PartitionStateOK PartitionState = "ok"
	// PartitionStateInvalid indicates the partition has invalid parquet files
	PartitionStateInvalid PartitionState = "invalid"
	// PartitionStateInProgress indicates a collection is in progress
	PartitionStateInProgress PartitionState = "in-progress"
)

// PartitionStateInfo represents the state information for a partition
type PartitionStateInfo struct {
	State   PartitionState `json:"state"`
	LastDay time.Time      `json:"last_day"`
	Message string         `json:"message,omitempty"`
	PID     int            `json:"pid,omitempty"`
}

// RepositoryState represents the state of the repository
type RepositoryState struct {
	// map of partition name to state info
	Partitions map[string]*PartitionStateInfo `json:"partitions"`
	statePath  string
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

// SetPartitionState sets the state for a partition in memory
func (rs *RepositoryState) SetPartitionState(partition string, state PartitionState, lastDay time.Time, message string, pid int) {
	rs.Partitions[partition] = &PartitionStateInfo{
		State:   state,
		LastDay: lastDay,
		Message: message,
		PID:     pid,
	}
}

// GetPartitionState gets the state for a partition from memory
func (rs *RepositoryState) GetPartitionState(partition string) *PartitionStateInfo {
	return rs.Partitions[partition]
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
