package collector

import (
	"context"
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
	table string
	// use to signal that we are complete
	completionChan chan error
}

func newExecution(part *config.Partition) *execution {
	e := &execution{
		partition:      part.UnqualifiedName,
		table:          part.TableName,
		plugin:         part.Plugin.Alias,
		state:          ExecutionState_PENDING,
		completionChan: make(chan error, 1),
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
	e.completionChan <- err
}

func (e *execution) complete() bool {
	return e.state == ExecutionState_COMPLETE || e.state == ExecutionState_ERROR
}

func (e *execution) waitForCompletion(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-e.completionChan:
		return err
	}
}
