package parquet

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// FileRootProvider provides a unique file root for parquet files
// based on the current time to the nanosecond.
// If multiple files are created in the same nanosecond, the provider will increment the time by a nanosecond
// to ensure the file root is unique.
type FileRootProvider struct {
	// the last time a filename was provided
	lastTime time.Time
	// mutex
	mutex *sync.Mutex

	executionId string
}

func newFileRootProvider(executionId string) *FileRootProvider {
	return &FileRootProvider{
		executionId: executionId,
		mutex:       &sync.Mutex{},
	}
}

// GetFileRoot returns a unique file root for a parquet file
// format is "data_<timestamp>_<microseconds>"
func (p *FileRootProvider) GetFileRoot() string {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()
	if now.Sub(p.lastTime) < time.Microsecond {
		slog.Debug("incrementing time")
		now = now.Add(time.Microsecond)
	}
	p.lastTime = now

	return fmt.Sprintf("data_%s_%s_%06d", p.executionId, now.Format("20060102150405"), now.Nanosecond()/1000)
}
