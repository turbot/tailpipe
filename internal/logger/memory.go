package logger

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"
)

var (
	processMemStatsOnce sync.Once
	basicMemStatsOnce   sync.Once
	memStatsMutex       sync.Mutex
)

func init() {
	// Delete the files at startup
	_ = os.Remove("process_memory_stats.csv")
	_ = os.Remove("basic_memory_stats.csv")
}

// WriteHeapSnapshot writes a detailed heap profile that can be analyzed with pprof
func WriteHeapSnapshot(label string) {
	filename := fmt.Sprintf("heap-%s-%s.prof", label, time.Now().Format("20060102-150405"))
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create heap profile: %v", err)
		return
	}
	defer f.Close()

	// Force garbage collection before taking the snapshot
	runtime.GC()

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Printf("Failed to write heap profile: %v", err)
	}
	log.Printf("Wrote heap profile to %s", filename)
}

// StartCPUProfile starts CPU profiling and returns a function to stop it
func StartCPUProfile(label string) func() {
	filename := fmt.Sprintf("cpu-%s-%s.prof", label, time.Now().Format("20060102-150405"))
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create CPU profile: %v", err)
		return func() {}
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		log.Printf("Failed to start CPU profile: %v", err)
		f.Close()
		return func() {}
	}

	log.Printf("Started CPU profile, writing to %s", filename)

	return func() {
		pprof.StopCPUProfile()
		f.Close()
		log.Printf("Stopped CPU profile")
	}
}

// StartTrace starts execution tracing and returns a function to stop it
func StartTrace(label string) func() {
	filename := fmt.Sprintf("trace-%s-%s.out", label, time.Now().Format("20060102-150405"))
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create trace file: %v", err)
		return func() {}
	}

	if err := trace.Start(f); err != nil {
		log.Printf("Failed to start trace: %v", err)
		f.Close()
		return func() {}
	}

	log.Printf("Started trace, writing to %s", filename)

	return func() {
		trace.Stop()
		f.Close()
		log.Printf("Stopped trace")
	}
}

// humanizeBytes converts bytes to a human readable string
func humanizeBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
