package resmon

// #include <mach/mach.h>
import "C"
import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"
)

// GetFilesDiskUsage calculates the total disk usage of files matching the given patterns
// Returns total size in bytes and error if any
func GetFilesDiskUsage(patterns []string) (uint64, error) {
	var totalSize uint64
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, pattern := range patterns {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			matches, err := filepath.Glob(p)
			if err != nil {
				log.Printf("Error matching pattern %s: %v", p, err)
				return
			}

			for _, match := range matches {
				info, err := os.Stat(match)
				if err != nil {
					log.Printf("Error getting file info for %s: %v", match, err)
					continue
				}
				if !info.IsDir() {
					mu.Lock()
					totalSize += uint64(info.Size())
					mu.Unlock()
				}
			}
		}(pattern)
	}

	wg.Wait()
	return totalSize, nil
}

// MonitorResourceUsage starts a goroutine that periodically logs memory statistics
// until the done channel is closed. The interval parameter specifies how ofte	n
// to log the statistics.

type ResourceUsageStats struct {
	MaxMemoryUsageMb uint64 // Memory usage in megabytes
	MaxDiskUsageMb   uint64
}

func MonitorResourceUsage(ctx context.Context, interval time.Duration, filePatterns []string) chan ResourceUsageStats {
	var resultChan = make(chan ResourceUsageStats, 1)
	var maxMem uint64 = 0
	var maxDisk uint64 = 0
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
				stats, err := GetProcessMemoryInfo(os.Getpid())
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

				diskUsage, err := GetFilesDiskUsage(filePatterns)
				if err != nil {
					log.Printf("Failed to get disk usage: %v", err)
					continue
				}
				if diskUsage > maxDisk {
					maxDisk = diskUsage
				}

				//LogProcessMemStats(label, memInfo, m)
			case <-ctx.Done():
				resultChan <- ResourceUsageStats{
					MaxMemoryUsageMb: maxMem / (1024 * 1024),
					MaxDiskUsageMb:   maxDisk / (1024 * 1024),
				}
				return
			}
		}
	}()
	return resultChan
}

func GetProcessMemoryInfo(pid int) (map[string]uint64, error) {
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
