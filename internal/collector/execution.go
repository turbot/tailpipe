package collector

// enum of execution states
type ExecutionState int

const (
	ExecutionState_PENDING ExecutionState = iota
	ExecutionState_STARTED ExecutionState = iota
	ExecutionState_COMPLETE
	ExecutionState_ERROR
)

// an execution represents the execution of a collection
type execution struct {
	plugin string
	state  ExecutionState
	id     string
	// the chunks written
	chunkCount int32
	// total rows returned by the plugin
	totalRows int64
}
