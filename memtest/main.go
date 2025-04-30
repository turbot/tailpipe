package main

// #include <mach/mach.h>
import "C"
import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb/v2"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ensureOutputDirectory creates the output directory if it doesn't exist
func ensureOutputDirectory() error {
	outputDir := "./output"
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		return os.MkdirAll(outputDir, 0755)
	}
	return nil
}

const (
	// Use a relative path for the output directory
	queryFormat = `copy (select * from read_ndjson('%s'))
	to './output' (
			format parquet,
			partition_by (tp_index,tp_date),
			overwrite_or_ignore,
			return_files true
	);`
	// Query to get memory usage
	memoryQuery = "SELECT temporary_storage_bytes FROM duckdb_memory() WHERE tag = 'COLUMN_DATA'"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <filename>", os.Args[0])
	}

	// Validate file exists
	filename := os.Args[1]
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		log.Fatalf("File does not exist: %s", filename)
	}

	// Parse filename to get parameters
	baseName := strings.TrimSuffix(filepath.Base(filename), ".jsonl")
	params := strings.Split(baseName, "_")
	if len(params) < 5 {
		log.Fatalf("Skipping invalid filename format: %s (expected at least 5 parts, got %d)", filename, len(params))
		return
	}

	// Extract parameters from filename
	rows, _ := strconv.Atoi(strings.TrimSuffix(params[1], "rows"))
	cols, _ := strconv.Atoi(strings.TrimSuffix(params[2], "cols"))
	indexes, _ := strconv.Atoi(strings.TrimSuffix(params[3], "indexes"))
	dates, _ := strconv.Atoi(strings.TrimSuffix(params[4], "dates"))
	partitions := indexes * dates

	// Ensure output directory exists
	if err := ensureOutputDirectory(); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	resultChan := monitorMemoryUsage(ctx, 250*time.Millisecond)

	// Run the query
	_, err = runQueryAndGetMemory(db, filename)
	if err != nil {
		log.Fatalf("Failed to get memory usage: %v", err)
	}

	cancel() // Signal memory monitoring to stop

	// Ensure channel read doesn't block indefinitely
	select {
	case maxMemory := <-resultChan:
		fmt.Printf("%d, %d, %d, %d\n", rows, cols, partitions, maxMemory/(1024*1024))
	case <-time.After(5 * time.Second):
		log.Fatal("Timed out waiting for memory results")
	}

	//processBadFiles()

	//// Create CSV file with datetime stamp
	//timestamp := time.Now().Format("20060102_150405")
	//csvFilename := fmt.Sprintf("memory_results_%s.csv", timestamp)
	//csvFile, err := os.Create(csvFilename)
	//if err != nil {
	//	log.Fatalf("Failed to create CSV file: %v", err)
	//}
	//defer csvFile.Close()
	//
	//writer := csv.NewWriter(csvFile)
	//defer writer.Flush()
	//
	//// Write CSV header
	//header := []string{"rows", "columns", "indexes", "dates", "partitions", "filename", "memory_mb", "query_error"}
	//if err := writer.Write(header); err != nil {
	//	log.Fatalf("Failed to write CSV header: %v", err)
	//}
	//
	//// Get all generated files
	//files, err := filepath.Glob("testdata/generated/*.jsonl")
	//if err != nil {
	//	log.Fatalf("Failed to find generated files: %v", err)
	//}
	//// Sort in reverse order
	//sort.Sort(sort.Reverse(sort.StringSlice(files)))
	//
	//// Process each file
	//for _, file := range files {
	//	processFile(file, writer)
	//}

}

//
//func processBadFiles() {
//
//	var files = []string{
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/bad.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_9000.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_7500.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_5000.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_2500.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_1000.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_500.jsonl",
//		"/Users/kai/Dev/github/turbot/tailpipe/memtest/testdata/subsets/bad_100.jsonl",
//	}
//
//	for _, file := range files {
//		db, err := sql.Open("duckdb", ":memory:")
//		if err != nil {
//			log.Fatalf("Failed to open connection: %v", err)
//		}
//		memoryMB, queryErr := runQueryAndGetMemory(db, file)
//		db.Close()
//
//		if queryErr != nil {
//			fmt.Printf("Failed to get memory usage for %s: %v", file, queryErr)
//		} else {
//			fmt.Printf("Memory usage for %s: %.2f MB\n", file, memoryMB)
//		}
//	}
//}
//
//func processFile(fileName string, writer *csv.Writer) {
//	db, err := sql.Open("duckdb", ":memory:")
//	if err != nil {
//		log.Fatalf("Failed to open connection: %v", err)
//	}
//
//	file, err := filepath.Abs(fileName)
//	if err != nil {
//		log.Printf("Failed to get absolute path for %s: %v", file, err)
//		return
//	}
//	// Parse filename to get parameters
//	baseName := strings.TrimSuffix(filepath.Base(file), ".jsonl")
//	params := strings.Split(baseName, "_")
//	if len(params) < 5 {
//		log.Printf("Skipping invalid filename format: %s (expected at least 5 parts, got %d)", file, len(params))
//		return
//	}
//
//	// Extract parameters from filename
//	rows, _ := strconv.Atoi(strings.TrimSuffix(params[1], "rows"))
//	cols, _ := strconv.Atoi(strings.TrimSuffix(params[2], "cols"))
//	indexes, _ := strconv.Atoi(strings.TrimSuffix(params[3], "indexes"))
//	dates, _ := strconv.Atoi(strings.TrimSuffix(params[4], "dates"))
//
//	// Get memory usage and error
//	memoryMB, queryErr := runQueryAndGetMemory(db, file)
//	if queryErr != nil {
//		log.Printf("Failed to get memory usage for %s: %v", fileName, queryErr)
//	} else {
//		fmt.Printf("Memory usage for %s: %.2f MB\n", fileName, memoryMB)
//	}
//
//	// Calculate memory string
//	var memoryStr string
//	if queryErr != nil {
//		memoryStr = ""
//	} else {
//		memoryStr = fmt.Sprintf("%.2f", memoryMB)
//	}
//
//	// Write to CSV
//	record := []string{
//		strconv.Itoa(rows),
//		strconv.Itoa(cols),
//		strconv.Itoa(indexes),
//		strconv.Itoa(dates),
//		strconv.Itoa(indexes * dates),
//		fileName,
//		memoryStr,
//		fmt.Sprintf("%v", queryErr),
//	}
//	if err := writer.Write(record); err != nil {
//		log.Printf("Failed to write record for %s: %v", fileName, err)
//	}
//}

func runQueryAndGetMemory(db *sql.DB, filename string) (float64, error) {
	// Validate database connection
	if err := db.Ping(); err != nil {
		return 0, fmt.Errorf("database connection error: %v", err)
	}

	// Get memory usage before query
	var memoryBefore int64
	err := db.QueryRow(memoryQuery).Scan(&memoryBefore)
	if err != nil {
		return 0, fmt.Errorf("failed to get initial memory usage: %v", err)
	}

	// Prepare and run the query
	query := fmt.Sprintf(queryFormat, filename)

	// Use context-aware Exec to allow for timeout/cancellation
	_, err = db.Exec(query)
	if err != nil {
		return 0, fmt.Errorf("failed to run query on %s: %v", filename, err)
	}

	// Get memory usage after query
	var memoryAfter int64
	err = db.QueryRow(memoryQuery).Scan(&memoryAfter)
	if err != nil {
		return 0, fmt.Errorf("failed to get final memory usage: %v", err)
	}

	// Convert bytes to MB
	memoryMB := float64(memoryAfter-memoryBefore) / (1024 * 1024)
	//fmt.Printf("Memory usage for %s: %.2f MB (before: %d bytes, after: %d bytes)\n",
	//	filepath.Base(filename), memoryMB, memoryBefore, memoryAfter)
	return memoryMB, nil
}
