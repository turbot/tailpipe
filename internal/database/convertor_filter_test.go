package database

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe-plugin-sdk/table"
	"github.com/turbot/tailpipe/internal/config"
)

// filterTestResult is what the converter reported to its status func over a whole run
type filterTestResult struct {
	rowCount, failedRows int64
	errs                 []error
	savedRows            int64
}

// runFilterConversion converts one chunk of `rows` JSONL rows for a partition with the given filter, in a real
// in-memory DuckDB, and returns what the converter reported through its status func and what ended up in the table
func runFilterConversion(t *testing.T, filter string, rows int) filterTestResult {
	return runConversion(t, filter, rows, true)
}

// runConversion is runFilterConversion, optionally without creating the target table (so the insert fails)
func runConversion(t *testing.T, filter string, rows int, createTable bool) filterTestResult {
	t.Helper()
	var sb strings.Builder
	for i := 1; i <= rows; i++ {
		sb.WriteString(`{"id": ` + string(rune('0'+i)) + "}\n")
	}
	return runConversionRows(t, filter, sb.String(), createTable)
}

// runConversionRows converts one chunk with the given JSONL content
func runConversionRows(t *testing.T, filter, jsonl string, createTable bool) filterTestResult {
	t.Helper()
	return runConversionRowsIn(t, t.TempDir(), filter, jsonl, createTable)
}

// runConversionRowsIn is runConversionRows with the chunk files read from (and removed from) the given source dir
func runConversionRowsIn(t *testing.T, sourceDir, filter, jsonl string, createTable bool) filterTestResult {
	t.Helper()

	sqlDb, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	sqlDb.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDb.Close() })
	db := &DuckDb{DB: sqlDb}

	// write a chunk of JSONL, one row per line
	chunkFile := filepath.Join(sourceDir, table.ExecutionIdToJsonlFileName("exec", 0))
	if err := os.WriteFile(chunkFile, []byte(jsonl), 0600); err != nil {
		t.Fatal(err)
	}

	tableSchema := &schema.TableSchema{
		Name: "filter_test",
		Columns: []*schema.ColumnSchema{
			{SourceName: "id", ColumnName: "id", Type: "bigint", Required: true},
		},
	}
	conversionSchema := schema.NewConversionSchema(tableSchema)

	if createTable {
		if _, err := db.Exec(`create table filter_test (id bigint)`); err != nil {
			t.Fatal(err)
		}
	}

	partition := &config.Partition{
		TableName:     "filter_test",
		Filter:        filter,
		TpIndexColumn: `'default'`,
	}

	var (
		mu  sync.Mutex
		res filterTestResult
	)
	w := &Converter{
		executionId:      "exec",
		sourceDir:        sourceDir,
		Partition:        partition,
		tableSchema:      tableSchema,
		conversionSchema: conversionSchema,
		db:               db,
		statusFunc: func(rowCount, failedRows int64, errs ...error) {
			mu.Lock()
			defer mu.Unlock()
			res.rowCount, res.failedRows = rowCount, failedRows
			res.errs = append(res.errs, errs...)
		},
	}
	w.readJsonQueryFormat = buildReadJsonQueryFormat(conversionSchema, partition)
	w.viewQueryOnce.Do(func() {})

	if err := w.AddChunk("exec", 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.WaitForConversions(ctx); err != nil {
		t.Fatalf("conversion did not finish: %v", err)
	}

	if createTable {
		if err := db.QueryRow(`select count(*) from filter_test`).Scan(&res.savedRows); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return res
}

// a filter naming a column that does not exist must surface DuckDB's error naming the column, and report every
// row of the chunk as an error rather than as saved
func TestConverter_FilterNamingMissingColumn(t *testing.T) {
	res := runFilterConversion(t, "invalid_column = 1", 5)

	if res.savedRows != 0 || res.rowCount != 0 {
		t.Errorf("expected no rows saved, got table=%d reported=%d", res.savedRows, res.rowCount)
	}
	if len(res.errs) != 1 {
		t.Fatalf("expected one error reported, got %d: %v", len(res.errs), res.errs)
	}
	if msg := res.errs[0].Error(); !strings.Contains(msg, `Referenced column "invalid_column" not found in FROM clause!`) {
		t.Errorf("error does not name the missing column: %s", msg)
	}
	if res.failedRows != 5 {
		t.Errorf("expected the 5 rows of the chunk reported as errors, got %d", res.failedRows)
	}
}

// the happy side: a filter naming a real column collects as before - the matching rows are saved, no errors
func TestConverter_FilterNamingExistingColumn(t *testing.T) {
	res := runFilterConversion(t, "id % 2 = 0", 5)

	if len(res.errs) != 0 {
		t.Errorf("expected no errors, got %v", res.errs)
	}
	if res.failedRows != 0 {
		t.Errorf("expected no failed rows, got %d", res.failedRows)
	}
	if res.savedRows != 2 || res.rowCount != 2 {
		t.Errorf("expected 2 rows saved (ids 2 and 4), got table=%d reported=%d", res.savedRows, res.rowCount)
	}
}

// an error that is not a conversion error (it carries no row count) must not reset the count of rows already
// reported as failed
func TestConverter_AddJobErrorsKeepsFailedRowCount(t *testing.T) {
	var lastFailed int64 = -1
	w := &Converter{statusFunc: func(_, failed int64, _ ...error) { lastFailed = failed }}

	w.addJobErrors(NewConversionError(errors.New("bad chunk"), 5, "a.jsonl"))
	if lastFailed != 5 {
		t.Fatalf("expected 5 failed rows, got %d", lastFailed)
	}
	w.addJobErrors(errors.New("plain error"))
	if lastFailed != 5 {
		t.Errorf("a plain error reset the failed row count to %d, expected 5", lastFailed)
	}
	w.addJobErrors(NewConversionError(errors.New("another bad chunk"), 3, "b.jsonl"))
	if lastFailed != 8 {
		t.Errorf("expected 8 failed rows, got %d", lastFailed)
	}
}

// a failed insert into the destination table reports every row of the chunk as an error too
func TestConverter_InsertFailureCountsRows(t *testing.T) {
	res := runConversion(t, "", 5, false)

	if len(res.errs) != 1 {
		t.Fatalf("expected one error reported, got %d: %v", len(res.errs), res.errs)
	}
	if res.failedRows != 5 || res.rowCount != 0 {
		t.Errorf("expected 5 failed rows and 0 saved, got failed=%d saved=%d", res.failedRows, res.rowCount)
	}
}

// a chunk with a row failing validation and then a failed insert reports the whole chunk as errors, and shows
// both errors
func TestConverter_ValidationFailureThenInsertFailure(t *testing.T) {
	res := runConversionRows(t, "", `{"id": 1}`+"\n"+`{"id": null}`+"\n"+`{"id": 3}`+"\n"+`{"id": 4}`+"\n"+`{"id": 5}`+"\n", false)

	if len(res.errs) != 1 {
		t.Fatalf("expected one error reported, got %d: %v", len(res.errs), res.errs)
	}
	msg := res.errs[0].Error()
	if !strings.Contains(msg, "failed validation") || !strings.Contains(msg, "failed to insert rows") {
		t.Errorf("expected both the validation and the insert error, got: %s", msg)
	}
	if res.failedRows != 5 || res.rowCount != 0 {
		t.Errorf("expected 5 failed rows and 0 saved, got failed=%d saved=%d", res.failedRows, res.rowCount)
	}
}

// the happy side of the same: a row failing validation, with a good insert, is only that row's error
func TestConverter_ValidationFailureOnly(t *testing.T) {
	res := runConversionRows(t, "", `{"id": 1}`+"\n"+`{"id": null}`+"\n"+`{"id": 3}`+"\n", true)

	if res.failedRows != 1 || res.rowCount != 2 || res.savedRows != 2 {
		t.Errorf("expected 1 failed and 2 saved, got failed=%d reported=%d table=%d", res.failedRows, res.rowCount, res.savedRows)
	}
}

// a collection whose chunk files are in a folder with a quote in its name (the SQL-escaped filename is not the path on
// disk) must delete each chunk file, report no error and keep every row
func TestConverter_ChunkFileInFolderWithQuote(t *testing.T) {
	sourceDir := filepath.Join(t.TempDir(), "o'brien")
	if err := os.Mkdir(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	logs := captureLog(t)

	res := runConversionRowsIn(t, sourceDir, "", `{"id": 1}`+"\n"+`{"id": 2}`+"\n", true)

	if len(res.errs) != 0 {
		t.Errorf("expected no errors, got: %v", res.errs)
	}
	if res.rowCount != 2 || res.savedRows != 2 {
		t.Errorf("expected 2 rows saved, got reported=%d table=%d", res.rowCount, res.savedRows)
	}
	if entries, err := os.ReadDir(sourceDir); err != nil || len(entries) != 0 {
		t.Errorf("expected the chunk file to be deleted, found %v (err %v)", entries, err)
	}
	if strings.Contains(logs.String(), "Failed to delete") {
		t.Errorf("unexpected delete error logged: %s", logs.String())
	}
}
