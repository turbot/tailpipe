package parquet

import (
	"log/slog"
	"sync/atomic"
)

// newWorkerFunc is a ctro for new workser - we pass this to the fileJobPool to create new workers
type newWorkerFunc[T any] func(chan fileJob[T], chan jobGroupError, string, string, *FileRootProvider) (worker, error)

// worker is the interface that all workers must implement
type worker interface {
	start()
}

// fileWorkerBase is a base class which implements the basic worker operations
// this must be embedded in a real worker whie provides the doWorkFunc and (optionally) closeFunc
type fileWorkerBase[T any] struct {
	// channel to receive jobs from the writer
	jobChan chan fileJob[T]
	// channel to send errors to the writer
	errorChan chan jobGroupError

	// source file location
	sourceDir string
	// dest file location
	destDir string

	// functions overridden by the real worker
	doWorkFunc func(job fileJob[T]) error
	closeFunc  func()
}

func newWorker[T any](jobChan chan fileJob[T], errorChan chan jobGroupError, sourceDir, destDir string) fileWorkerBase[T] {
	return fileWorkerBase[T]{
		jobChan:    jobChan,
		errorChan:  errorChan,
		sourceDir:  sourceDir,
		destDir:    destDir,
		doWorkFunc: func(job fileJob[T]) error { panic("doWorkFunc must be implemented by worker implementation") },
		closeFunc:  func() {},
	}
}

// this is the worker function run by all workers, which all read from the jobGroup channel
func (w *fileWorkerBase[T]) start() {
	slog.Debug("worker start")

	// loop until we are closed
	for job := range w.jobChan {
		// ok we have a job

		if err := w.doWorkFunc(job); err != nil {
			slog.Error("worker failed to process job", "error", err)
			// send the error to the writer
			w.errorChan <- jobGroupError{job.groupId, err}
			continue
		}
		// increment the completion count
		atomic.AddInt32(job.completionCount, 1)

		// log the completion count
		if *job.completionCount%100 == 0 {
			slog.Debug("jobGroup completion count", "jobGroup", job.groupId, "count", *job.completionCount)
		}

	}

	// we are done
	w.closeFunc()
}
