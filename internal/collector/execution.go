package collector

import (
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

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
	id         string
	collection string
	plugin     string
	state      ExecutionState
	// the chunks written
	chunkCount int32
	// total rows returned by the plugin
	totalRows int64
	timing    types.TimingMap
}
