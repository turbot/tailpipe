package parquet

import "sync"

// the jobGroup struct represents all the conversions that need to be done for a single 'jobGroup'
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type jobGroup[T any] struct {
	id string
	// the file chunks numbers available to process
	chunks    []int
	chunkLock sync.RWMutex
	// the index into 'chunks' of the next chunk number to process
	nextChunkIndex int
	// the number of chunks processed so far
	completionCount int32

	// sync.Cond to wait for the next chunkNumber to be available
	chunkWrittenSignal *sync.Cond

	// error which occurred during execution
	errors     []error
	errorsLock sync.RWMutex
	// channel to mark jobGroup completion
	done chan struct{}
	// group payload
	payload T
}

func newJobGroup[T any](id string, payload T) *jobGroup[T] {
	return &jobGroup[T]{
		id:                 id,
		chunkWrittenSignal: sync.NewCond(&sync.Mutex{}),
		done:               make(chan struct{}),
		payload:            payload,
	}
}
