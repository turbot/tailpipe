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
	"unsafe"
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

// MonitorMemoryUsage starts a goroutine that periodically logs memory statistics
// until the done channel is closed. The interval parameter specifies how ofte	n
// to log the statistics.
func monitorMemoryUsage(ctx context.Context, interval time.Duration) chan uint64 {
	var resultChan = make(chan uint64)
	var maxMem uint64 = 0
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Get current process
		//p, err := process.NewProcess(int32(os.Getpid()))
		//if err != nil {
		//	fmt.Printf("Failed to get process: %v", err)
		//	return
		//}

		for {
			select {
			case <-ticker.C:
				//// Get detailed memory info from the OS
				//memInfo, err := p.MemoryInfo()
				//if err != nil {
				//	log.Printf("Failed to get memory info: %v", err)
				//	continue
				//}
				//
				//if memInfo.RSS > maxMem {
				//	maxMem = memInfo.RSS
				//}
				//// Get Go's memory stats for comparison
				//var m runtime.MemStats
				//runtime.ReadMemStats(&m)
				//
				//// Calculate total memory including heap and external allocations
				//totalMem := memInfo.RSS + uint64(m.Sys-m.HeapSys)

				//if totalMem > maxMem {
				//	maxMem = totalMem
				//}
				//mem, err := getActualMemoryUsage()
				//if err != nil {
				//	log.Printf("Failed to get process memory: %v", err)
				//	continue
				//}
				stats, err := getProcessMemoryInfo(os.Getpid())
				if err != nil {
					log.Printf("Failed to get process memory: %v", err)
					continue
				}
				totalMemory := stats["resident_size"] + stats["compressed"]
				//fmt.Printf("Memory usage - Resident: %d MB, Compressed: %d MB, total: %d MB\n",
				//	stats["resident_size"]/(1024*1024),
				//	stats["compressed"]/(1024*1024),
				//	totalMemory/(1024*1024))

				if totalMemory > maxMem {
					maxMem = totalMemory
				}

				//LogProcessMemStats(label, memInfo, m)
			case <-ctx.Done():
				resultChan <- maxMem
				return
			}
		}
	}()
	return resultChan
}

func getProcessMemoryInfo(pid int) (map[string]uint64, error) {
	var task C.task_t

	// Get the task port for the specified PID
	kernReturn := C.task_for_pid(C.mach_task_self_, C.int(pid), &task)
	if kernReturn != C.KERN_SUCCESS {
		return nil, fmt.Errorf("failed to get task for pid %d: %d", pid, kernReturn)
	}
	defer C.mach_port_deallocate(C.mach_task_self_, task)

	// Get task memory info
	var taskInfo C.task_vm_info_data_t
	var count = C.mach_msg_type_number_t(C.TASK_VM_INFO_COUNT)

	kernReturn = C.task_info(
		task,
		C.TASK_VM_INFO,
		(*C.integer_t)(unsafe.Pointer(&taskInfo)),
		&count,
	)

	if kernReturn != C.KERN_SUCCESS {
		return nil, fmt.Errorf("failed to get task info: %d", kernReturn)
	}

	stats := map[string]uint64{
		"resident_size": uint64(taskInfo.resident_size),
		"virtual_size":  uint64(taskInfo.virtual_size),
		"compressed":    uint64(taskInfo.compressed),
	}

	return stats, nil
}
