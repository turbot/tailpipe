package parquet

import (
	"log/slog"
	"sync/atomic"
)

// newWorkerFunc is a ctro for new workser - we pass this to the fileJobPool to create new workers
type newWorkerFunc func(jobChan chan fileJob, errorChan chan error, sourceDir, destDir string) (worker, error)

// worker is the interface that all workers must implement
type worker interface {
	start()
}

// fileWorkerBase is a base class which implements the basic worker operations
// this must be embedded in a real worker whie provides the doWorkFunc and (optionally) closeFunc
type fileWorkerBase struct {
	// channel to receive jobs from the writer
	jobChan chan fileJob
	// channel to send errors to the writer
	errorChan chan error

	// source file location
	sourceDir string
	// dest file location
	destDir string

	// functions overriden by the real worker
	doWorkFunc func(job fileJob)
	closeFunc  func()
}

func newWorker(jobChan chan fileJob, errorChan chan error, sourceDir, destDir string) fileWorkerBase {
	return fileWorkerBase{
		jobChan:    jobChan,
		errorChan:  errorChan,
		sourceDir:  sourceDir,
		destDir:    destDir,
		doWorkFunc: func(job fileJob) { panic("doWorkFunc must be implemented by worker implementation") },
		closeFunc:  func() {},
	}
}

// this is the worker function run by all workers, which all read from the jobGroup channel
func (w *fileWorkerBase) start() {
	slog.Debug("worker start")

	// loop until we are closed
	for job := range w.jobChan {
		// ok we have a job
		slog.Debug("worker received job", "groupId", job.groupId, "chunkNumber", job.chunkNumber)

		w.doWorkFunc(job)
		// increment the completion count
		atomic.AddInt32(job.completionCount, 1)
		slog.Debug("worker completed job", "groupId", job.groupId, "chunkNumber", job.chunkNumber)
	}

	// we are done
	w.closeFunc()
}
