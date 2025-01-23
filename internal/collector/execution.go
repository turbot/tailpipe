package collector

import (
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/types"
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
	rowsReceived    int64
	executionTiming types.Timing
	// timing for the plugin operations
	pluginTiming types.TimingCollection
	// pluginTiming for the parquet conversion
	conversionTiming types.Timing
	table            string
}

func newExecution(executionId string, part *config.Partition) *execution {
	e := &execution{
		id:        executionId,
		partition: part.UnqualifiedName,
		table:     part.TableName,
		plugin:    part.Plugin.Alias,
		state:     ExecutionState_PENDING,
	}
	e.executionTiming.TryStart("total time")
	return e
}

// getTiming returns the timing for the execution,
// adding the conversion and full execution timing to the end of the plugin timing list
func (e *execution) getTiming() types.TimingCollection {
	// TODO #timing a nice way of doing this
	res := e.pluginTiming
	if e.conversionTiming.Operation != "" {
		res = append(res, e.conversionTiming)
	}
	return append(res, e.executionTiming)
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
	e.executionTiming.End = time.Now()
	e.conversionTiming.End = time.Now()
}
