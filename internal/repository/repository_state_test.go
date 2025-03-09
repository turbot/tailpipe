package repository

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/stretchr/testify/assert"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func TestRepositoryState_GetPartitionState(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name   string
		fields fields
		args   args
		want   PartitionStateInfo
	}{
		{
			name: "get existing partition",
			fields: fields{
				Partitions: map[string]PartitionStateInfo{
					"test": {
						State:           CollectionStateIdle,
						InvalidFromDate: now,
						PID:             123,
					},
				},
			},
			args: args{
				partition: "test",
			},
			want: PartitionStateInfo{
				State:           CollectionStateIdle,
				InvalidFromDate: now,
				PID:             123,
			},
		},
		{
			name: "get non-existent partition",
			fields: fields{
				Partitions: map[string]PartitionStateInfo{
					"test": {
						State:           CollectionStateIdle,
						InvalidFromDate: now,
					},
				},
			},
			args: args{
				partition: "nonexistent",
			},
			want: PartitionStateInfo{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &RepositoryState{
				Partitions: tt.fields.Partitions,
			}
			got := rs.GetPartitionState(tt.args.partition)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LoadPartitionState() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepositoryState_SetPartitionState(t *testing.T) {
	now := time.Now()
	tempDir := t.TempDir()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "set new partition state",
			fields: fields{
				Partitions: make(map[string]PartitionStateInfo),
				statePath:  filepath.Join(tempDir, "state.json"),
			},
			args: args{
				partition: "test",
				state:     CollectionStateIdle,
				lastDay:   now,
				pid:       123,
			},
			wantErr: false,
		},
		{
			name: "update existing partition state",
			fields: fields{
				Partitions: map[string]PartitionStateInfo{
					"test": {
						State:           CollectionStateInCollecting,
						InvalidFromDate: now.Add(-24 * time.Hour),
					},
				},
				statePath: filepath.Join(tempDir, "state.json"),
			},
			args: args{
				partition: "test",
				state:     CollectionStateIdle,
				lastDay:   now,
				pid:       123,
			},
			wantErr: false,
		},
		{
			name: "set state with empty message",
			fields: fields{
				Partitions: make(map[string]PartitionStateInfo),
				statePath:  filepath.Join(tempDir, "state.json"),
			},
			args: args{
				partition: "test",
				state:     CollectionStateIdle,
				lastDay:   now,
				pid:       123,
			},
			wantErr: false,
		},
		{
			name: "set state with zero PID",
			fields: fields{
				Partitions: make(map[string]PartitionStateInfo),
				statePath:  filepath.Join(tempDir, "state.json"),
			},
			args: args{
				partition: "test",
				state:     CollectionStateIdle,
				lastDay:   now,
				pid:       0,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &RepositoryState{
				Partitions: tt.fields.Partitions,
				statePath:  tt.fields.statePath,
			}
			rs.SetPartitionCollectionState(tt.args.partition, tt.args.state, tt.args.pid)
			rs.SetPartitionInvalidFromDate(tt.args.partition, tt.args.lastDay)
			if err := rs.Save(); (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				var empty PartitionStateInfo
				info := rs.GetPartitionState(tt.args.partition)
				if info == empty {
					t.Error("SetPartitionState() partition not found after setting")
				}
				if info.State != tt.args.state {
					t.Errorf("SetPartitionState() state = %v, want %v", info.State, tt.args.state)
				}
				if !info.InvalidFromDate.Equal(tt.args.lastDay) {
					t.Errorf("SetPartitionState() invalidFromDate = %v, want %v", info.InvalidFromDate, tt.args.lastDay)
				}
				if info.PID != tt.args.pid {
					t.Errorf("SetPartitionState() pid = %v, want %v", info.PID, tt.args.pid)
				}
			}
		})
	}
}

// TestRepositoryState_Integration tests the full workflow including file operations
func TestRepositoryState_Integration(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "state.json")

	// Override the default state path for testing
	originalPath := filepaths.GetLocalRepositoryStatePath
	filepaths.GetLocalRepositoryStatePath = func() string { return statePath }
	defer func() { filepaths.GetLocalRepositoryStatePath = originalPath }()

	// Test setting and getting partition state
	partitionFullName := "aws_cloudtrail_log.partition1"
	lastDay := time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC)
	pid := os.Getpid()

	// Set initial state
	err := UpdateAndSavePartitionState(partitionFullName, CollectionStateInCollecting, lastDay, pid)
	if err != nil {
		t.Fatalf("Failed to set partition state: %v", err)
	}

	// Verify state was persisted correctly
	info, err := LoadPartitionState(partitionFullName)
	if err != nil {
		t.Fatalf("Failed to get partition state: %v", err)
	}

	if info.State != CollectionStateInCollecting {
		t.Errorf("Expected state %s, got %s", CollectionStateInCollecting, info.State)
	}
	if !info.InvalidFromDate.Equal(lastDay) {
		t.Errorf("Expected invalidFromDate %v, got %v", lastDay, info.InvalidFromDate)
	}
	if info.PID != pid {
		t.Errorf("Expected PID %d, got %d", pid, info.PID)
	}

	// Test updating state
	err = UpdateAndSavePartitionState(partitionFullName, CollectionStateIdle, lastDay, pid)
	if err != nil {
		t.Fatalf("Failed to update partition state: %v", err)
	}

	// Verify the update was persisted
	info, err = LoadPartitionState(partitionFullName)
	if err != nil {
		t.Fatalf("Failed to get updated partition state: %v", err)
	}

	if info.State != CollectionStateIdle {
		t.Errorf("Expected state %s, got %s", CollectionStateIdle, info.State)
	}
	if !info.InvalidFromDate.Equal(lastDay) {
		t.Errorf("Expected invalidFromDate %v, got %v", lastDay, info.InvalidFromDate)
	}
	if info.PID != pid {
		t.Errorf("Expected PID %d, got %d", pid, info.PID)
	}

	// Test handling of aborted collection
	// Set state to in-progress with a non-existent PID
	err = UpdateAndSavePartitionState(partitionFullName, CollectionStateInCollecting, lastDay, 999999)
	if err != nil {
		t.Fatalf("Failed to set partition state: %v", err)
	}

	// Get state - should be marked as invalid
	info, err = LoadPartitionState(partitionFullName)
	if err != nil {
		t.Fatalf("Failed to get partition state: %v", err)
	}

	if info.State != CollectionStateIdle {
		t.Errorf("Expected state %s, got %s", CollectionStateIdle, info.State)
	}
	if info.PID != 999999 {
		t.Errorf("Expected PID %d, got %d", 999999, info.PID)
	}
}

type fields struct {
	Partitions map[string]PartitionStateInfo
	statePath  string
}

type args struct {
	partition string
	state     PartitionState
	lastDay   time.Time
	pid       int
}

// TestRepositoryState_FileLocking tests that file locking prevents concurrent access
func TestRepositoryState_FileLocking(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "state.json")

	// Create channels for coordination
	lockAcquired := make(chan struct{})
	lockReleased := make(chan struct{})
	secondGoroutineDone := make(chan struct{})

	// Start a goroutine that will hold the lock for a while
	go func() {
		rs := &RepositoryState{
			Partitions: make(map[string]PartitionStateInfo),
			statePath:  statePath,
		}
		err := rs.withFileLock(func() error {
			// Signal that we've acquired the lock
			close(lockAcquired)
			// Wait for signal to release lock
			<-lockReleased
			return nil
		})
		if err != nil {
			t.Errorf("Error in lock-holding goroutine: %v", err)
		}
	}()

	// Wait for the first goroutine to acquire the lock
	<-lockAcquired

	// Try to access the file from another goroutine - should block
	go func() {
		rs := &RepositoryState{
			Partitions: make(map[string]PartitionStateInfo),
			statePath:  statePath,
		}
		// Create a context with a short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Try to acquire the lock with timeout
		done := make(chan struct{})
		go func() {
			err := rs.withFileLock(func() error {
				return nil
			})
			if err != nil {
				t.Errorf("Unexpected error in second goroutine: %v", err)
			}
			close(done)
		}()

		// Wait for either timeout or lock acquisition
		select {
		case <-ctx.Done():
			// This is what we expect - the lock acquisition should be blocked
			t.Log("Lock acquisition was blocked as expected")
		case <-done:
			t.Error("Lock acquisition succeeded when it should have been blocked")
		}
		close(secondGoroutineDone)
	}()

	// Wait a bit to ensure the second goroutine has started and attempted to acquire the lock
	time.Sleep(200 * time.Millisecond)

	// Signal the first goroutine to release the lock
	close(lockReleased)

	// Wait for the second goroutine to finish
	<-secondGoroutineDone
}

func TestLoadRepositoryState(t *testing.T) {
	type args struct {
		statePath string
	}
	tests := []struct {
		name    string
		args    args
		want    *RepositoryState
		wantErr bool
	}{
		{
			name: "load state in existing directory",
			args: args{
				statePath: filepath.Join(t.TempDir(), "state.json"),
			},
			want: &RepositoryState{
				Partitions: make(map[string]PartitionStateInfo),
			},
			wantErr: false,
		},
		{
			name: "load state in non-existent directory",
			args: args{
				statePath: filepath.Join(t.TempDir(), "newdir", "state.json"),
			},
			want: &RepositoryState{
				Partitions: make(map[string]PartitionStateInfo),
			},
			wantErr: false,
		},
		{
			name: "load state with empty path",
			args: args{
				statePath: "",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary state file for testing
			if tt.args.statePath != "" {
				dir := filepath.Dir(tt.args.statePath)
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatalf("Failed to create directory: %v", err)
				}
				if err := os.WriteFile(tt.args.statePath, []byte("{}"), 0644); err != nil {
					t.Fatalf("Failed to create state file: %v", err)
				}
			}

			// Override the default state path for testing
			originalPath := filepaths.GetLocalRepositoryStatePath
			filepaths.GetLocalRepositoryStatePath = func() string { return tt.args.statePath }
			defer func() { filepaths.GetLocalRepositoryStatePath = originalPath }()

			got, err := loadRepositoryState()
			if (err != nil) != tt.wantErr {
				t.Errorf("loadRepositoryState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if got.statePath != tt.args.statePath {
				t.Errorf("loadRepositoryState() statePath = %v, want %v", got.statePath, tt.args.statePath)
			}
			if !reflect.DeepEqual(got.Partitions, tt.want.Partitions) {
				t.Errorf("loadRepositoryState() Partitions = %v, want %v", got.Partitions, tt.want.Partitions)
			}
		})
	}
}

func TestFindEarliestTempFile(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create partition config
	block := &hcl.Block{
		Labels: []string{"aws_account", "123456789"},
	}
	partitionResource, _ := config.NewPartition(block, "partition.aws_account.123456789")
	partition := partitionResource.(*config.Partition)

	// Create test files with different dates
	testFiles := []struct {
		date     time.Time
		index    string
		expected bool // whether this should be the earliest
	}{
		{
			date:     time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
			index:    "1",
			expected: true, // earliest date
		},
		{
			date:     time.Date(2024, 3, 16, 0, 0, 0, 0, time.UTC),
			index:    "2",
			expected: false,
		},
		{
			date:     time.Date(2024, 3, 17, 0, 0, 0, 0, time.UTC),
			index:    "3",
			expected: false,
		},
	}

	// Create the files
	for _, tf := range testFiles {
		// Create a file with the date and index in the path
		filename := filepath.Join(dataDir,
			"tp_table="+partition.TableName,
			"tp_partition="+partition.GetUnqualifiedName(),
			"tp_date="+tf.date.Format("2006-01-02"),
			"tp_index="+tf.index,
			"file.parquet.tmp")

		// Create empty file
		if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filename, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		// Set file modification time to match the date
		if err := os.Chtimes(filename, tf.date, tf.date); err != nil {
			t.Fatal(err)
		}
	}

	// Test finding earliest file
	earliest := findEarliestTempFile(partition, dataDir)
	assert.Equal(t, time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), earliest)
}
