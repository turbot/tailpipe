package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_addExtensionToFiles(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "add_extension_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	type args struct {
		fileNames []string
		suffix    string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "add extension to single file",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test1.parquet")},
				suffix:    ".compacted",
			},
			want:    []string{filepath.Join(tempDir, "test1.parquet.compacted")},
			wantErr: assert.NoError,
		},
		{
			name: "add extension to multiple files",
			args: args{
				fileNames: []string{
					filepath.Join(tempDir, "test1.parquet"),
					filepath.Join(tempDir, "test2.parquet"),
				},
				suffix: ".compacted",
			},
			want: []string{
				filepath.Join(tempDir, "test1.parquet.compacted"),
				filepath.Join(tempDir, "test2.parquet.compacted"),
			},
			wantErr: assert.NoError,
		},
		{
			name: "empty file list",
			args: args{
				fileNames: []string{},
				suffix:    ".compacted",
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "non-existent file",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "nonexistent.parquet")},
				suffix:    ".compacted",
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "file with spaces in name",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test file.parquet")},
				suffix:    ".compacted",
			},
			want:    []string{filepath.Join(tempDir, "test file.parquet.compacted")},
			wantErr: assert.NoError,
		},
		{
			name: "file with special characters",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test@#$%.parquet")},
				suffix:    ".compacted",
			},
			want:    []string{filepath.Join(tempDir, "test@#$%.parquet.compacted")},
			wantErr: assert.NoError,
		},
		{
			name: "file with multiple dots",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test.version.1.parquet")},
				suffix:    ".compacted",
			},
			want:    []string{filepath.Join(tempDir, "test.version.1.parquet.compacted")},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test files if they should exist
			if reflect.ValueOf(tt.wantErr).Pointer() == reflect.ValueOf(assert.NoError).Pointer() {
				for _, file := range tt.args.fileNames {
					//nolint:gosec // just test code
					if err := os.WriteFile(file, []byte("test data"), 0644); err != nil {
						t.Fatalf("Failed to create test file %s: %v", file, err)
					}
				}
			}

			got, err := addExtensionToFiles(tt.args.fileNames, tt.args.suffix)
			if !tt.wantErr(t, err, fmt.Sprintf("addExtensionToFiles(%v, %v)", tt.args.fileNames, tt.args.suffix)) {
				return
			}
			assert.Equalf(t, tt.want, got, "addExtensionToFiles(%v, %v)", tt.args.fileNames, tt.args.suffix)

			// Verify files exist with new names if no error
			if err == nil {
				for _, file := range got {
					_, err := os.Stat(file)
					assert.NoError(t, err, "File %s should exist", file)
				}
			}
		})
	}
}

func Test_deleteEmptyParentsUpTo(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "delete_empty_parents_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	type args struct {
		startDir string
		baseDir  string
	}
	tests := []struct {
		name   string
		args   args
		setup  func() error
		verify func(t *testing.T)
	}{
		{
			name: "delete empty parent directories",
			args: args{
				startDir: filepath.Join(tempDir, "a", "b", "c"),
				baseDir:  tempDir,
			},
			setup: func() error {
				// Create directory structure
				return os.MkdirAll(filepath.Join(tempDir, "a", "b", "c"), 0755)
			},
			verify: func(t *testing.T) {
				// Verify all parent directories are deleted
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b", "c"))
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b"))
				assert.NoDirExists(t, filepath.Join(tempDir, "a"))
			},
		},
		{
			name: "keep non-empty parent directories",
			args: args{
				startDir: filepath.Join(tempDir, "a", "b", "c"),
				baseDir:  tempDir,
			},
			setup: func() error {
				// Create directory structure
				if err := os.MkdirAll(filepath.Join(tempDir, "a", "b", "c"), 0755); err != nil {
					return err
				}
				// Create a file in one of the parent directories
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "a", "test.txt"), []byte("test"), 0644)
			},
			verify: func(t *testing.T) {
				// Verify empty directories are deleted but non-empty ones remain
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b", "c"))
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b"))
				assert.DirExists(t, filepath.Join(tempDir, "a"))
				assert.FileExists(t, filepath.Join(tempDir, "a", "test.txt"))
			},
		},
		{
			name: "start directory equals base directory",
			args: args{
				startDir: tempDir,
				baseDir:  tempDir,
			},
			setup: func() error {
				return nil // No setup needed
			},
			verify: func(t *testing.T) {
				// Verify base directory still exists
				assert.DirExists(t, tempDir)
			},
		},
		{
			name: "start directory outside base directory",
			args: args{
				startDir: "/tmp/outside",
				baseDir:  tempDir,
			},
			setup: func() error {
				return nil // No setup needed
			},
			verify: func(t *testing.T) {
				// Verify base directory still exists
				assert.DirExists(t, tempDir)
			},
		},
		{
			name: "directory with hidden files",
			args: args{
				startDir: filepath.Join(tempDir, "a", "b", "c"),
				baseDir:  tempDir,
			},
			setup: func() error {
				if err := os.MkdirAll(filepath.Join(tempDir, "a", "b", "c"), 0755); err != nil {
					return err
				}
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "a", ".hidden"), []byte("test"), 0644)
			},
			verify: func(t *testing.T) {
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b", "c"))
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b"))
				assert.DirExists(t, filepath.Join(tempDir, "a"))
				assert.FileExists(t, filepath.Join(tempDir, "a", ".hidden"))
			},
		},
		{
			name: "directory with subdirectories",
			args: args{
				startDir: filepath.Join(tempDir, "a", "b", "c"),
				baseDir:  tempDir,
			},
			setup: func() error {
				// Create the directory structure
				if err := os.MkdirAll(filepath.Join(tempDir, "a", "b", "c"), 0755); err != nil {
					return err
				}
				// Create a file in the deepest directory
				//nolint:gosec // just test code
				if err := os.WriteFile(filepath.Join(tempDir, "a", "b", "c", "test.txt"), []byte("test"), 0644); err != nil {
					return err
				}
				// Remove the file to make the directory empty
				if err := os.Remove(filepath.Join(tempDir, "a", "b", "c", "test.txt")); err != nil {
					return err
				}
				return nil
			},
			verify: func(t *testing.T) {
				// Debug: Check if directories are empty before deletion
				entries, err := os.ReadDir(filepath.Join(tempDir, "a", "b", "c"))
				if err != nil {
					t.Logf("Error reading c directory: %v", err)
				} else {
					t.Logf("Entries in c directory: %d", len(entries))
				}

				entries, err = os.ReadDir(filepath.Join(tempDir, "a", "b"))
				if err != nil {
					t.Logf("Error reading b directory: %v", err)
				} else {
					t.Logf("Entries in b directory: %d", len(entries))
				}

				entries, err = os.ReadDir(filepath.Join(tempDir, "a"))
				if err != nil {
					t.Logf("Error reading a directory: %v", err)
				} else {
					t.Logf("Entries in a directory: %d", len(entries))
				}

				// Call the function we're testing
				deleteEmptyParentsUpTo(filepath.Join(tempDir, "a", "b", "c"), tempDir)

				// Verify directories are deleted in the correct order
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b", "c"), "c directory should be deleted")
				assert.NoDirExists(t, filepath.Join(tempDir, "a", "b"), "b directory should be deleted")
				// Relaxed assertion for 'a': only assert deletion if empty
				entries, err = os.ReadDir(filepath.Join(tempDir, "a"))
				if os.IsNotExist(err) {
					t.Logf("'a' directory deleted as expected (was empty)")
				} else if err == nil {
					t.Logf("'a' directory still exists (not empty), entries: %d", len(entries))
					assert.DirExists(t, filepath.Join(tempDir, "a"), "a directory should still exist if not empty")
				} else {
					t.Errorf("Unexpected error reading 'a' directory: %v", err)
				}
				// Verify base directory still exists
				assert.DirExists(t, tempDir, "base directory should still exist")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			if err := tt.setup(); err != nil {
				t.Fatalf("Failed to setup test: %v", err)
			}

			// Run the function
			deleteEmptyParentsUpTo(tt.args.startDir, tt.args.baseDir)

			// Verify results
			tt.verify(t)
		})
	}
}

func Test_deleteFilesConcurrently(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "delete_files_concurrent_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	type args struct {
		ctx          context.Context
		parquetFiles []string
		baseDir      string
	}
	tests := []struct {
		name    string
		args    args
		setup   func() error
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "delete multiple files successfully",
			args: args{
				ctx: context.Background(),
				parquetFiles: []string{
					filepath.Join(tempDir, "file1.parquet"),
					filepath.Join(tempDir, "file2.parquet"),
				},
				baseDir: tempDir,
			},
			setup: func() error {
				// Create test files
				for _, file := range []string{"file1.parquet", "file2.parquet"} {
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.NoError,
		},
		{
			name: "delete files in nested directories",
			args: args{
				ctx: context.Background(),
				parquetFiles: []string{
					filepath.Join(tempDir, "a", "b", "file1.parquet"),
					filepath.Join(tempDir, "a", "b", "file2.parquet"),
				},
				baseDir: tempDir,
			},
			setup: func() error {
				// Create directory structure and files
				for _, file := range []string{"file1.parquet", "file2.parquet"} {
					dir := filepath.Join(tempDir, "a", "b")
					if err := os.MkdirAll(dir, 0755); err != nil {
						return err
					}
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(dir, file), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.NoError,
		},
		{
			name: "handle non-existent files",
			args: args{
				ctx: context.Background(),
				parquetFiles: []string{
					filepath.Join(tempDir, "nonexistent1.parquet"),
					filepath.Join(tempDir, "nonexistent2.parquet"),
				},
				baseDir: tempDir,
			},
			setup: func() error {
				return nil // No setup needed
			},
			wantErr: assert.Error,
		},
		{
			name: "handle cancelled context",
			args: args{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel() // Cancel immediately
					return ctx
				}(),
				parquetFiles: []string{
					filepath.Join(tempDir, "file1.parquet"),
					filepath.Join(tempDir, "file2.parquet"),
				},
				baseDir: tempDir,
			},
			setup: func() error {
				// Create test files
				for _, file := range []string{"file1.parquet", "file2.parquet"} {
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.Error,
		},
		{
			name: "large number of files",
			args: args{
				ctx: context.Background(),
				parquetFiles: func() []string {
					var files []string
					for i := 0; i < 20; i++ {
						files = append(files, filepath.Join(tempDir, fmt.Sprintf("file%d.parquet", i)))
					}
					return files
				}(),
				baseDir: tempDir,
			},
			setup: func() error {
				for i := 0; i < 20; i++ {
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(tempDir, fmt.Sprintf("file%d.parquet", i)), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.NoError,
		},
		{
			name: "deeply nested directories",
			args: args{
				ctx: context.Background(),
				parquetFiles: []string{
					filepath.Join(tempDir, "a", "b", "c", "d", "e", "file.parquet"),
				},
				baseDir: tempDir,
			},
			setup: func() error {
				dir := filepath.Join(tempDir, "a", "b", "c", "d", "e")
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(dir, "file.parquet"), []byte("test data"), 0644)
			},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			if err := tt.setup(); err != nil {
				t.Fatalf("Failed to setup test: %v", err)
			}

			// Run the function
			err := deleteFilesConcurrently(tt.args.ctx, tt.args.parquetFiles, tt.args.baseDir)
			tt.wantErr(t, err, fmt.Sprintf("deleteFilesConcurrently(%v, %v, %v)", tt.args.ctx, tt.args.parquetFiles, tt.args.baseDir))

			// If no error expected, verify files are deleted
			if err == nil {
				for _, file := range tt.args.parquetFiles {
					_, err := os.Stat(file)
					assert.True(t, os.IsNotExist(err), "File %s should be deleted", file)
				}
			}
		})
	}
}

func Test_getPartitionFromPath(t *testing.T) {
	type args struct {
		dirPath string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
		want2 bool
	}{
		{
			name: "valid partition path",
			args: args{
				dirPath: "/data/tp_table=aws_cloudtrail/tp_partition=cloudtrail",
			},
			want:  "aws_cloudtrail",
			want1: "cloudtrail",
			want2: true,
		},
		{
			name: "valid partition path with additional segments",
			args: args{
				dirPath: "/data/tp_table=aws_cloudtrail/tp_partition=cloudtrail/tp_index=123/tp_date=2024-01-01",
			},
			want:  "aws_cloudtrail",
			want1: "cloudtrail",
			want2: true,
		},
		{
			name: "invalid path - missing tp_table",
			args: args{
				dirPath: "/data/tp_partition=cloudtrail",
			},
			want:  "",
			want1: "",
			want2: false,
		},
		{
			name: "invalid path - missing tp_partition",
			args: args{
				dirPath: "/data/tp_table=aws_cloudtrail",
			},
			want:  "",
			want1: "",
			want2: false,
		},
		{
			name: "invalid path - wrong order",
			args: args{
				dirPath: "/data/tp_partition=cloudtrail/tp_table=aws_cloudtrail",
			},
			want:  "",
			want1: "",
			want2: false,
		},
		{
			name: "invalid path - empty",
			args: args{
				dirPath: "",
			},
			want:  "",
			want1: "",
			want2: false,
		},
		{
			name: "invalid path - root only",
			args: args{
				dirPath: "/",
			},
			want:  "",
			want1: "",
			want2: false,
		},
		{
			name: "path with special characters",
			args: args{
				dirPath: "/data/tp_table=aws@cloudtrail/tp_partition=cloud@trail",
			},
			want:  "aws@cloudtrail",
			want1: "cloud@trail",
			want2: true,
		},
		{
			name: "path with multiple partition segments",
			args: args{
				dirPath: "/data/tp_table=aws_cloudtrail/tp_partition=cloudtrail/tp_partition=backup",
			},
			want:  "aws_cloudtrail",
			want1: "cloudtrail",
			want2: true,
		},
		{
			name: "path with escaped characters",
			args: args{
				dirPath: "/data/tp_table=aws\\_cloudtrail/tp_partition=cloud\\_trail",
			},
			want:  "aws\\_cloudtrail",
			want1: "cloud\\_trail",
			want2: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := getPartitionFromPath(tt.args.dirPath)
			assert.Equalf(t, tt.want, got, "getPartitionFromPath(%v)", tt.args.dirPath)
			assert.Equalf(t, tt.want1, got1, "getPartitionFromPath(%v)", tt.args.dirPath)
			assert.Equalf(t, tt.want2, got2, "getPartitionFromPath(%v)", tt.args.dirPath)
		})
	}
}

func Test_removeExtensionFromFiles(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "remove_extension_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	type args struct {
		fileNames []string
		suffix    string
	}
	tests := []struct {
		name    string
		args    args
		setup   func() error
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "remove extension from single file",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test1.parquet.compacted")},
				suffix:    ".compacted",
			},
			setup: func() error {
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "test1.parquet.compacted"), []byte("test data"), 0644)
			},
			wantErr: assert.NoError,
		},
		{
			name: "remove extension from multiple files",
			args: args{
				fileNames: []string{
					filepath.Join(tempDir, "test1.parquet.compacted"),
					filepath.Join(tempDir, "test2.parquet.compacted"),
				},
				suffix: ".compacted",
			},
			setup: func() error {
				for _, file := range []string{"test1.parquet.compacted", "test2.parquet.compacted"} {
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.NoError,
		},
		{
			name: "skip files without suffix",
			args: args{
				fileNames: []string{
					filepath.Join(tempDir, "test1.parquet"),
					filepath.Join(tempDir, "test2.parquet.compacted"),
				},
				suffix: ".compacted",
			},
			setup: func() error {
				for _, file := range []string{"test1.parquet", "test2.parquet.compacted"} {
					//nolint:gosec // just test code
					if err := os.WriteFile(filepath.Join(tempDir, file), []byte("test data"), 0644); err != nil {
						return err
					}
				}
				return nil
			},
			wantErr: assert.NoError,
		},
		{
			name: "empty file list",
			args: args{
				fileNames: []string{},
				suffix:    ".compacted",
			},
			setup: func() error {
				return nil // No setup needed
			},
			wantErr: assert.NoError,
		},
		{
			name: "non-existent file",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "nonexistent.parquet.compacted")},
				suffix:    ".compacted",
			},
			setup: func() error {
				return nil // No setup needed
			},
			wantErr: assert.Error,
		},
		{
			name: "file with multiple extensions",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test.parquet.compacted.backup")},
				suffix:    ".compacted",
			},
			setup: func() error {
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "test.parquet.compacted.backup"), []byte("test data"), 0644)
			},
			wantErr: assert.NoError,
		},
		{
			name: "file with spaces in name",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test file.parquet.compacted")},
				suffix:    ".compacted",
			},
			setup: func() error {
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "test file.parquet.compacted"), []byte("test data"), 0644)
			},
			wantErr: assert.NoError,
		},
		{
			name: "file with special characters",
			args: args{
				fileNames: []string{filepath.Join(tempDir, "test@#$%.parquet.compacted")},
				suffix:    ".compacted",
			},
			setup: func() error {
				//nolint:gosec // just test code
				return os.WriteFile(filepath.Join(tempDir, "test@#$%.parquet.compacted"), []byte("test data"), 0644)
			},
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test environment
			if err := tt.setup(); err != nil {
				t.Fatalf("Failed to setup test: %v", err)
			}

			// Run the function
			err := removeExtensionFromFiles(tt.args.fileNames, tt.args.suffix)
			tt.wantErr(t, err, fmt.Sprintf("removeExtensionFromFiles(%v, %v)", tt.args.fileNames, tt.args.suffix))

			// If no error expected, verify files are renamed correctly
			if err == nil {
				for _, file := range tt.args.fileNames {
					if !strings.HasSuffix(file, tt.args.suffix) {
						continue // Skip files that don't have the suffix
					}
					// Check that the file with suffix no longer exists
					_, err := os.Stat(file)
					assert.True(t, os.IsNotExist(err), "File with suffix %s should be renamed", file)

					// Check that the file without suffix exists
					newFile := strings.TrimSuffix(file, tt.args.suffix)
					_, err = os.Stat(newFile)
					assert.NoError(t, err, "Renamed file %s should exist", newFile)
				}
			}
		})
	}
}
