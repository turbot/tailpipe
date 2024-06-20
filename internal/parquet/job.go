package parquet

import "sync"

// the collection struct represents all the conversions that need to be done for a single 'collection'
// it therefore has a unique execution id, and will potentially involve the conversion of multiple JSONL files
// each file is assumed to have the filename format <execution_id>_<chunkNumber>.jsonl
// so when new input files are available, we simply store the chunkNumber number
type collection struct {
	// The execution id
	executionId string
	// the file chunks numbers available to process
	chunks    []int
	chunkLock sync.RWMutex
	// the index into 'chunks' of the next chunkNumber to process
	nextChunkIndex int
	// the number of chunks converted so far
	chunksConverted int32

	// sync.Cond to wait for the next chunkNumber to be available
	chunkWrittenSignal *sync.Cond

	// channel to mark collection completion
	done chan struct{}
}

func newCollection(executionId string) *collection {
	return &collection{
		executionId:        executionId,
		chunkWrittenSignal: sync.NewCond(&sync.Mutex{}),
		done:               make(chan struct{}),
	}
}

type job struct {
	executionID    string
	chunkNumber    int
	collectionType string
	// pointer to the completion count
	completionCount *int32
}
