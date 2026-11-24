package database

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// lockedBuffer is a log buffer that is safe to read while the converter's processor goroutine is still writing:
// the processor logs one last line after the wait for conversions has returned
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog redirects slog to a buffer for the test
func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// a directory with a file in it cannot be removed by os.Remove: a real (not "no such file") delete failure
func undeletablePath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "chunk.jsonl")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "child"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestRemoveChunkFiles_DeletesFilesOnce(t *testing.T) {
	logs := captureLog(t)
	dir := t.TempDir()
	// a quote in the path is escaped for SQL in the filename list, but the file on disk has the single quote
	names := []string{filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "it's.jsonl")}
	var filenames []string
	for _, n := range names {
		if err := os.WriteFile(n, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		filenames = append(filenames, strings.ReplaceAll(n, "'", "''"))
	}

	if err := removeChunkFiles(filenames, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, n := range names {
		if _, err := os.Stat(n); !os.IsNotExist(err) {
			t.Errorf("%s was not deleted", n)
		}
	}
	// deleting the same list again: every file is already gone, which is neither logged nor returned
	if err := removeChunkFiles(filenames, nil); err != nil {
		t.Fatalf("already-deleted files must not be an error: %v", err)
	}
	if logs.String() != "" {
		t.Errorf("nothing should be logged for files which are already gone, got: %s", logs.String())
	}
}

func TestRemoveChunkFiles_AlreadyGoneKeepsConversionResult(t *testing.T) {
	logs := captureLog(t)
	missing := filepath.Join(t.TempDir(), "gone.jsonl")

	if err := removeChunkFiles([]string{missing}, nil); err != nil {
		t.Errorf("a successful conversion must stay successful, got: %v", err)
	}
	convErr := errors.New("conversion failed")
	if err := removeChunkFiles([]string{missing}, convErr); err != convErr {
		t.Errorf("the conversion error must be returned unchanged, got: %v", err)
	}
	if logs.String() != "" {
		t.Errorf("unexpected log output: %s", logs.String())
	}
}

func TestRemoveChunkFiles_RealDeleteFailure(t *testing.T) {
	bad := undeletablePath(t)
	good := filepath.Join(t.TempDir(), "good.jsonl")
	if err := os.WriteFile(good, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("logs the delete's own error and returns it when the conversion worked", func(t *testing.T) {
		logs := captureLog(t)
		err := removeChunkFiles([]string{bad}, nil)
		if err == nil {
			t.Fatal("expected the delete error to be returned")
		}
		if !strings.Contains(logs.String(), "Failed to delete file after processing") || !strings.Contains(logs.String(), "directory not empty") {
			t.Errorf("expected the delete error to be logged, got: %s", logs.String())
		}
		if strings.Contains(logs.String(), "conversion failed") {
			t.Errorf("the log must not carry the conversion error: %s", logs.String())
		}
	})

	t.Run("logs the delete's error, not the conversion's, and keeps the conversion error", func(t *testing.T) {
		logs := captureLog(t)
		convErr := errors.New("conversion failed")
		if err := removeChunkFiles([]string{bad, good}, convErr); err != convErr {
			t.Errorf("the conversion error must take precedence, got: %v", err)
		}
		if strings.Contains(logs.String(), "conversion failed") || !strings.Contains(logs.String(), "directory not empty") {
			t.Errorf("the log must carry the delete's own error, got: %s", logs.String())
		}
		// a failure on one file does not stop the others being deleted
		if _, err := os.Stat(good); !os.IsNotExist(err) {
			t.Error("the remaining file was not deleted")
		}
	})
}
