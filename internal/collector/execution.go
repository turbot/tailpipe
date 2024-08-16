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

// an execution represents the execution of a collection
type execution struct {
	id         string
	collection string
	plugin     string
	state      ExecutionState
	// the chunks written
	chunkCount int32
	// total rows returned by the plugin
	totalRows       int64
	executionTiming types.Timing
	// timing for the plugin operations
	pluginTiming types.TimingCollection
	// pluginTiming for the parquet conversion
	conversionTiming types.Timing
}

func newExecution(executionId string, col *config.Collection) *execution {
	e := &execution{
		id:         executionId,
		collection: col.UnqualifiedName,
		plugin:     col.Plugin,
		state:      ExecutionState_PENDING,
	}
	e.executionTiming.TryStart("total time")
	return e
}

// getTiming returns the timing for the execution,
// adding the conversion and full execution timing to the end of the plugin timing list
func (e *execution) getTiming() types.TimingCollection {
	return append(e.pluginTiming, e.conversionTiming, e.executionTiming)
}

// set state to complete and set end time for the execution and the conversion timing
func (e *execution) done() {
	e.state = ExecutionState_COMPLETE
	e.executionTiming.End = time.Now()
	e.conversionTiming.End = time.Now()
}
