package collector

import (
	"github.com/turbot/tailpipe/internal/config"
)

// enum of execution states
type ExecutionState int

const (
	ExecutionState_PENDING ExecutionState = iota
	ExecutionState_STARTED ExecutionState = iota
	ExecutionState_COMPLETE
	ExecutionState_ERROR
)

// an execution represents the execution of a partition
type execution struct {
	id        string
	partition string
	plugin    string
	state     ExecutionState
	// if the execution state is in error, this is the error
	error error
	// the chunks written
	chunkCount int32
	// total rows returned by the plugin
	rowsReceived int64
	table        string
}

func newExecution(executionId string, part *config.Partition) *execution {
	e := &execution{
		id:        executionId,
		partition: part.UnqualifiedName,
		table:     part.TableName,
		plugin:    part.Plugin.Alias,
		state:     ExecutionState_PENDING,
	}

	return e
}

// set state to complete and set end time for the execution and the conversion timing
func (e *execution) done(err error) {
	// if the execution state is NOT already in error, set to complete
	if err != nil {
		e.state = ExecutionState_ERROR
		e.error = err
	} else if e.state != ExecutionState_ERROR {
		// if state has not already been set to error, set to complete
		e.state = ExecutionState_COMPLETE
	}
}
