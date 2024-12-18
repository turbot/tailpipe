package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/sethvargo/go-retry"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe/internal/collection_state"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

const eventBufferSize = 100
const parquetWorkerCount = 5
const executionMaxDuration = 2 * time.Hour

type Collector struct {
	Events chan *proto.Event

	pluginManager *plugin_manager.PluginManager
	// map of executions
	executions map[string]*execution
	// lock for executions
	executionsLock sync.RWMutex

	parquetWriter             *parquet.Writer
	collectionStateRepository *collection_state.Repository
	spinner                   *spinner.Spinner
	// the current plugin status - used to update the spinner
	status     status
	sourcePath string
}

func New(ctx context.Context) (*Collector, error) {
	sourcePath, err := filepaths.EnsureSourcePath()
	if err != nil {
		return nil, fmt.Errorf("failed to create source path: %w", err)
	}

	// get the data dir - this will already have been created by the config loader
	parquetPath := config.GlobalWorkspaceProfile.GetDataDir()

	c := &Collector{
		Events:     make(chan *proto.Event, eventBufferSize),
		executions: make(map[string]*execution),
		sourcePath: sourcePath,
	}

	// create a plugin manager
	c.pluginManager = plugin_manager.New()
	c.pluginManager.AddObserver(c)

	// create a parquet writer
	parquetWriter, err := parquet.NewWriter(sourcePath, parquetPath, parquetWorkerCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	c.parquetWriter = parquetWriter
	c.collectionStateRepository, err = collection_state.NewRepository()
	if err != nil {
		return nil, fmt.Errorf("failed to create collection state repository: %w", err)
	}

	// TODO #ui temp
	c.spinner = spinner.New(
		spinner.CharSets[14],
		100*time.Millisecond,
		spinner.WithHiddenCursor(true),
		spinner.WithWriter(os.Stdout),
	)

	// start listening to plugin event
	c.listenToEventsAsync(ctx)
	return c, nil
}

func (c *Collector) Close() {
	close(c.Events)
	c.spinner.Stop()
}

func (c *Collector) Collect(ctx context.Context, partition *config.Partition) error {
	c.spinner.Start()
	c.spinner.Suffix = " Collecting logs"

	// try to load collection state data
	collectionState, err := c.collectionStateRepository.Load(partition.UnqualifiedName)
	if err != nil {
		return fmt.Errorf("failed to load collection state data: %w", err)
	}

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, partition, c.sourcePath, collectionState)
	if err != nil {
		return fmt.Errorf("failed to collect: %w", err)
	}
	// set the schema on the parquet writer - NOTE: this may be dynamic (i.e. empty) or partial, in which case, we
	// will need to infer the schema from the first JSONL file
	c.parquetWriter.SetSchema(collectResponse.Schema)
	executionId := collectResponse.ExecutionId
	// add the execution to the map
	e := newExecution(executionId, partition)

	c.executionsLock.Lock()
	c.executions[executionId] = e
	c.executionsLock.Unlock()

	// update the status with the chunks written
	// TODO #design tactical push this from writer???
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
				rowCount, err := c.parquetWriter.GetRowCount(executionId)
				if err == nil {
					c.status.SetRowsConverted(rowCount)
					c.setStatusMessage()
				}
			}
		}
	}()

	// create jobPayload
	payload := parquet.JobPayload{
		Partition:            partition,
		SchemaFunc:           c.parquetWriter.GetSchema,
		UpdateActiveDuration: e.conversionTiming.UpdateActiveDuration,
	}

	// start the parquet writer job
	return c.parquetWriter.StartJobGroup(executionId, payload)
}

// Notify implements observer.Observer
// send an event down the channel to be picked up by the handlePluginEvent goroutine
func (c *Collector) Notify(event *proto.Event) {
	c.Events <- event
}

// handlePluginEvent handles an event from a plugin
func (c *Collector) handlePluginEvent(ctx context.Context, e *proto.Event) {
	// handlePluginEvent the event
	// switch based on the struct of the event
	switch e.GetEvent().(type) {
	case *proto.Event_StartedEvent:
		slog.Debug("Event_StartedEvent", "execution", e.GetStartedEvent().ExecutionId)
		executionId := e.GetStartedEvent().ExecutionId
		c.executionsLock.Lock()
		if e, ok := c.executions[executionId]; ok {
			e.state = ExecutionState_STARTED
		}
		c.executionsLock.Unlock()
	case *proto.Event_StatusEvent:
		c.status.UpdateWithPluginStatus(e.GetStatusEvent())
		c.setStatusMessage()
	case *proto.Event_ChunkWrittenEvent:
		ev := e.GetChunkWrittenEvent()

		executionId := ev.ExecutionId
		chunkNumber := int(ev.ChunkNumber)

		// retrieve the execution
		c.executionsLock.RLock()
		execution, ok := c.executions[executionId]
		c.executionsLock.RUnlock()
		if !ok {
			slog.Error("Event_ChunkWrittenEvent - execution not found", "execution", executionId)
			// TODO #errors x what to do with this error?  https://github.com/turbot/tailpipe/issues/35
			return
		}

		// set the conversion start time if it hasn't been set
		execution.conversionTiming.TryStart(constants.TimingConvert)

		// log every 100 chunks
		if ev.ChunkNumber%100 == 0 {
			slog.Debug("Event_ChunkWrittenEvent", "execution", ev.ExecutionId, "chunk", ev.ChunkNumber)
		}

		err := c.parquetWriter.AddJob(executionId, chunkNumber)
		if err != nil {
			slog.Error("failed to add chunk to parquet writer", "error", err)
			// TODO #errors x what to do with this error?  https://github.com/turbot/tailpipe/issues/35
		}
		// store collection state data
		if len(ev.CollectionState) > 0 {
			err = c.collectionStateRepository.Save(execution.partition, string(ev.CollectionState))
			if err != nil {
				slog.Error("failed to save collection state data", "error", err)
				// TODO #errors x what to do with this error?  https://github.com/turbot/tailpipe/issues/35
			}
		}
	case *proto.Event_CompleteEvent:
		ev := e.GetCompleteEvent()
		// TODO if no chunk written event was received, this currently stalls https://github.com/turbot/tailpipe/issues/7
		slog.Debug("Event_CompleteEvent", "execution", e.GetCompleteEvent().ExecutionId)

		completedEvent := e.GetCompleteEvent()

		// was there an error?
		if completedEvent.Error != "" {
			slog.Error("execution error", "execution", completedEvent.ExecutionId, "error", completedEvent.Error)
			// retrieve the execution
			c.executionsLock.RLock()
			execution, ok := c.executions[ev.ExecutionId]
			c.executionsLock.RUnlock()
			if !ok {
				slog.Error("Event_CompleteEvent - execution not found", "execution", ev.ExecutionId)
				return
			}
			execution.state = ExecutionState_ERROR
			execution.error = fmt.Errorf("plugin error: %s", completedEvent.Error)
		}
		// this event means all JSON files have been written - we need to wait for all to be converted to parquet
		// we then combine the parquet files into a single file

		// start thread waiting for execution to complete
		// - this will wait for all parquet files to be written, and will then combine these into a single parquet file
		// TODO #errors x what to do with an error here?  https://github.com/turbot/tailpipe/issues/35
		go func() {
			err := c.waitForExecution(ctx, completedEvent)
			if err != nil {
				slog.Error("error waiting for execution to complete", "error", err)
			}
		}()

	case *proto.Event_ErrorEvent:
		slog.Error("Event_ErrorEvent", "error", e.GetErrorEvent().Error)
		// TODO #errors x what to do with an error here?

	}
}

// WaitForCompletion waits for all collections to complete, then cleans up the collector - closes the file watcher
func (c *Collector) WaitForCompletion(ctx context.Context) {
	slog.Info("closing collector - wait for executions to complete")

	// wait for any ongoing partitions to complete
	err := c.waitForExecutions(ctx)
	if err != nil {
		// TODO #errors x https://github.com/turbot/tailpipe/issues/35
		slog.Error("error waiting for executions to complete", "error", err)
	}

	c.parquetWriter.Close()
	c.pluginManager.Close()
	c.collectionStateRepository.Close()

	// if inbox path is empty, remove it (ignore errors)
	_ = os.Remove(c.sourcePath)
}

func (c *Collector) StatusString() string {
	// TODO K we need to test multiple executions https://github.com/turbot/tailpipe/issues/71
	var str strings.Builder
	str.WriteString("Collection complete.\n\n")
	str.WriteString(c.status.String())
	str.WriteString("\n")
	// print out the execution status
	for _, e := range c.executions {
		if e.state == ExecutionState_ERROR {
			// if no rows were converted, just show the error
			if e.totalRows == 0 {
				str.Reset()
			}
			str.WriteString(fmt.Sprintf("Execution %s failed: %s\n", e.id, e.error))
		}
		//case ExecutionState_COMPLETE:
		//	str.WriteString(fmt.Sprintf("Execution %s complete\n", e.id))
	}
	return str.String()
}

func (c *Collector) TimingString() string {
	var str strings.Builder
	// print out the execution status
	for _, e := range c.executions {
		str.WriteString(e.getTiming().String())
		str.WriteString("\n")
	}
	return str.String()
}

// waitForExecution waits for the parquet writer to complete the conversion of the JSONL files to parquet
// it then sets the execution state to ExecutionState_COMPLETE
func (c *Collector) waitForExecution(ctx context.Context, ce *proto.EventComplete) error {
	slog.Info("waiting for execution to complete", "execution", ce.ExecutionId)

	// store the plugin pluginTiming for this execution
	c.setPluginTiming(ce.ExecutionId, ce.Timing)

	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 5 * time.Second
	c.executionsLock.Lock()
	e, ok := c.executions[ce.ExecutionId]
	c.executionsLock.Unlock()
	if !ok {
		slog.Error("waitForExecution - execution not found", "execution", ce.ExecutionId)
		return fmt.Errorf("execution not found: %s", ce.ExecutionId)
	}
	e.totalRows = ce.RowCount
	e.chunkCount = ce.ChunkCount

	if ce.ChunkCount == 0 && ce.RowCount == 0 {
		slog.Debug("waitForExecution - no chunks/rows to write", "execution", ce.ExecutionId)
		var err error
		if ce.Error != "" {
			slog.Warn("waitForExecution - plugin execution returned error", "execution", ce.ExecutionId, "error", ce.Error)
			err = errors.New(ce.Error)
		}
		e.done(err)
		return nil
	}

	// so there was no error

	err := retry.Do(ctx, retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		// check chunk count - ask the parquet writer how many chunks have been written
		chunksWritten, err := c.parquetWriter.GetChunksWritten(ce.ExecutionId)
		if err != nil {
			return fmt.Errorf("failed to get chunksWritten written: %w", err)
		}

		// if no chunks have been written, we are done
		if e.chunkCount == 0 {
			slog.Warn("waitForExecution - no chunks to write", "execution", e.id)
			return nil
		}

		slog.Debug("waitForExecution", "execution", e.id, "chunk written", chunksWritten, "total chunks", e.chunkCount)

		if chunksWritten < e.chunkCount {
			slog.Debug("waiting for parquet conversion", "execution", e.id, "chunks written", chunksWritten, "total chunksWritten", e.chunkCount)
			// not all chunks have been written
			return retry.RetryableError(fmt.Errorf("not all chunks have been written"))
		}
		// so we are done writing chunks - now update the db to add a view to this data
		// Open a DuckDB connection
		db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
		if err != nil {
			return err
		}
		defer db.Close()

		return database.AddTableView(ctx, e.table, db)
	})

	slog.Debug("waitForExecution - all chunks written", "execution", e.id)

	// mark execution as complete and record the end time
	e.done(err)

	// if there was an error, return it
	if err != nil {
		return err
	}

	// notify the writer that the collection is complete
	return c.parquetWriter.JobGroupComplete(ce.ExecutionId)
}

// waitForExecutions waits for ALL executions to have state ExecutionState_COMPLETE
func (c *Collector) waitForExecutions(ctx context.Context) error {
	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 5 * time.Second

	err := retry.Do(ctx, retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		c.executionsLock.RLock()
		defer c.executionsLock.RUnlock()
		for _, e := range c.executions {
			switch e.state {
			case ExecutionState_ERROR:
				return NewExecutionError(errors.New("execution in error state"), e.id)
			case ExecutionState_COMPLETE:

				return nil
			default:
				//slog.Debug("waiting for executions to complete", "execution", e.id, "state", e.state)
				return retry.RetryableError(NewExecutionError(errors.New("execution not complete"), e.id))
			}
		}

		// all complete
		return nil
	})
	if err != nil {
		if err.Error() == "execution not complete" {
			return fmt.Errorf("not all executions completed after %s", executionTimeout.String())
		}
		return err
	}
	return nil
}

func (c *Collector) listenToEventsAsync(ctx context.Context) {
	// TODO #control_flow do we need to consider end conditions here - check context or nil event? https://github.com/turbot/tailpipe/issues/8
	go func() {
		for event := range c.Events {
			c.handlePluginEvent(ctx, event)
		}
	}()
}

func (c *Collector) setStatusMessage() {
	c.spinner.Suffix = " " + c.status.String()
}

func (c *Collector) setPluginTiming(executionId string, timing []*proto.Timing) {
	c.executionsLock.Lock()
	defer c.executionsLock.Unlock()
	if e, ok := c.executions[executionId]; ok {
		e.pluginTiming = events.TimingCollectionFromProto(timing)
	}
}
