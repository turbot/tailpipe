package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/sethvargo/go-retry"
	sdkfilepaths "github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

const eventBufferSize = 100
const executionMaxDuration = 2 * time.Hour

type Collector struct {
	Events chan *proto.Event

	pluginManager *plugin_manager.PluginManager
	// the partition to collect
	partition *config.Partition
	// the execution
	execution *execution

	parquetWriter *parquet.ParquetJobPool

	// the current plugin status - used to update the spinner
	status status

	// the subdirectory of ~/.tailpipe/internal which will be used to store all file data for this collection
	// this will have the form ~/.tailpipe/internal/collection/<profile>/<pid>
	collectionTempDir string

	sourcePath string

	// bubble tea app
	app    *tea.Program
	cancel context.CancelFunc

	// errors which occurred during the collection
	errors []string
}

func New(pluginManager *plugin_manager.PluginManager, partition *config.Partition, cancel context.CancelFunc) (*Collector, error) {
	// get the temp data dir for this collection
	// - this is located  in ~/.turbot/internal/collection/<profile_name>/<pid>
	collectionTempDir := filepaths.GetCollectionTempDir()

	c := &Collector{
		Events:            make(chan *proto.Event, eventBufferSize),
		pluginManager:     pluginManager,
		collectionTempDir: collectionTempDir,
		partition:         partition,
		cancel:            cancel,
	}

	// create a plugin manager
	c.pluginManager.AddObserver(c)

	// get the JSONL path
	sourcePath, err := sdkfilepaths.EnsureJSONLPath(c.collectionTempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSONL path: %w", err)
	}
	c.sourcePath = sourcePath

	return c, nil
}

func (c *Collector) Close() {
	close(c.Events)

	if c.parquetWriter != nil {
		c.parquetWriter.Close()
	}

	if c.app != nil {
		c.app.Send(CollectionFinishedMsg{})
	}

	// if inbox path is empty, remove it (ignore errors)
	_ = os.Remove(c.sourcePath)

	// delete the collection temp dir
	_ = os.RemoveAll(c.collectionTempDir)
}

func (c *Collector) Collect(ctx context.Context, fromTime time.Time) error {
	if c.execution != nil {
		return errors.New("collection already in progress")
	}

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, c.partition, fromTime, c.collectionTempDir)
	if err != nil {
		return fmt.Errorf("failed to collect: %w", err)
	}

	resolvedFromTime := collectResponse.FromTime

	c.app = tea.NewProgram(newCollectionModel(c.partition.GetUnqualifiedName(), *resolvedFromTime))

	go func() {
		model, err := c.app.Run()
		if model.(collectionModel).cancelled {
			slog.Info("Collection UI returned cancelled")
			c.doCancel()
		}
		if err != nil {
			slog.Warn("Collection UI returned error", "error", err)
			c.doCancel()
		}

	}()

	// if there is a from time, add a filter to the partition - this will be used by the parquet writer
	if !resolvedFromTime.Time.IsZero() {
		c.partition.AddFilter(fmt.Sprintf("tp_timestamp >= '%s'", resolvedFromTime.Time.Format("2006-01-02T15:04:05")))
	}

	executionId := collectResponse.ExecutionId
	// add the execution to the map
	c.execution = newExecution(executionId, c.partition)

	// create a parquet writer
	parquetWriter, err := parquet.NewParquetJobPool(executionId, c.partition, c.sourcePath, collectResponse.Schema)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	c.parquetWriter = parquetWriter

	// update the status with the chunks written
	// TODO #design tactical push this from writer???
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
				c.updateConvertedStatus()
			}
		}
	}()

	// start listening to plugin event
	c.listenToEventsAsync(ctx)

	return nil
}

func (c *Collector) updateConvertedStatus() {
	rowCount, err := c.parquetWriter.GetRowCount()
	if err == nil {
		c.status.SetRowsConverted(rowCount)
		c.app.Send(c.status)
	}
}

// Notify implements observer.Observer
// send an event down the channel to be picked up by the handlePluginEvent goroutine
func (c *Collector) Notify(event *proto.Event) {
	// only send the event if the execution is not complete - this is to handle the case where it has
	// terminated with an error, causing the collector to close, closing the channel
	if !c.execution.complete() {
		c.Events <- event
	}
}

// handlePluginEvent handles an event from a plugin
func (c *Collector) handlePluginEvent(ctx context.Context, e *proto.Event) {
	// handlePluginEvent the event
	// switch based on the struct of the event
	switch e.GetEvent().(type) {
	case *proto.Event_StartedEvent:
		slog.Debug("Event_StartedEvent", "execution", e.GetStartedEvent().ExecutionId)
		c.execution.state = ExecutionState_STARTED
	case *proto.Event_StatusEvent:
		c.status.UpdateWithPluginStatus(e.GetStatusEvent())
		c.app.Send(c.status)
	case *proto.Event_ChunkWrittenEvent:
		ev := e.GetChunkWrittenEvent()

		executionId := ev.ExecutionId
		chunkNumber := int(ev.ChunkNumber)

		// log every 100 chunks
		if ev.ChunkNumber%100 == 0 {
			slog.Debug("Event_ChunkWrittenEvent", "execution", ev.ExecutionId, "chunk", ev.ChunkNumber)
		}

		err := c.parquetWriter.AddChunk(executionId, chunkNumber)
		if err != nil {
			slog.Error("failed to add chunk to parquet writer", "error", err)
			// TODO #errors what to do with this error?  https://github.com/turbot/tailpipe/issues/106
		}
	case *proto.Event_CompleteEvent:
		completedEvent := e.GetCompleteEvent()
		slog.Debug("Event_CompleteEvent", "execution", completedEvent.ExecutionId)

		// was there an error?
		if completedEvent.Error != "" {
			slog.Error("execution error", "execution", completedEvent.ExecutionId, "error", completedEvent.Error)
			// retrieve the execution
			c.execution.state = ExecutionState_ERROR
			c.execution.error = fmt.Errorf("plugin error: %s", completedEvent.Error)
		}
		// this event means all JSON files have been written - we need to wait for all to be converted to parquet
		// we then combine the parquet files into a single file

		// start thread waiting for execution to complete
		// - this will wait for all parquet files to be written, and will then combine these into a single parquet file
		// TODO #errors x what to do with an error here?  https://github.com/turbot/tailpipe/issues/106
		go func() {
			err := c.waitForConversions(ctx, completedEvent)
			if err != nil {
				slog.Error("error waiting for execution to complete", "error", err)
			}
		}()

	case *proto.Event_ErrorEvent:
		ev := e.GetErrorEvent()
		// TODO think about fatal vs non fatal errors https://github.com/turbot/tailpipe/issues/179
		// for now just store errors and display at end
		//c.execution.state = ExecutionState_ERROR
		//c.execution.error = fmt.Errorf("plugin error: %s", ev.Error)
		slog.Warn("plugin error", "execution", ev.ExecutionId, "error", ev.Error)
		c.errors = append(c.errors, ev.Error)
	}
}

func (c *Collector) StatusString() string {
	var str strings.Builder
	str.WriteString("Collection complete.\n\n")
	str.WriteString(c.status.String())
	str.WriteString("\n")
	// print out the execution status
	if c.execution.state == ExecutionState_ERROR {
		// if no rows were converted, just show the error
		if c.execution.rowsReceived == 0 {
			str.Reset()
		}
		str.WriteString(fmt.Sprintf("Execution %s failed: %s\n", c.execution.id, c.execution.error))
	}

	return str.String()
}

// WaitForCompletion waits for our execution to have state ExecutionState_COMPLETE
func (c *Collector) WaitForCompletion(ctx context.Context) error {
	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 100 * time.Millisecond

	err := retry.Do(ctx, retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		switch c.execution.state {
		case ExecutionState_ERROR:
			slog.Info("Execution in error state", "execution", c.execution.id)
			return NewExecutionError(fmt.Errorf("execution in error state: %s", c.execution.error.Error()), c.execution.id)
		case ExecutionState_COMPLETE:
			slog.Info("Execution complete", "execution", c.execution.id)
			return nil
		default:
			return retry.RetryableError(NewExecutionError(errors.New("execution not complete"), c.execution.id))
		}
	})
	if err != nil {
		if err.Error() == "execution not complete" {
			return fmt.Errorf("not all execution completed after %s", executionTimeout.String())
		}
		return err
	}

	return nil
}

// waitForConversions waits for the parquet writer to complete the conversion of the JSONL files to parquet
// it then sets the execution state to ExecutionState_COMPLETE
func (c *Collector) waitForConversions(ctx context.Context, ce *proto.EventComplete) (err error) {
	slog.Info("waiting for execution to complete", "execution", ce.ExecutionId, "chunks", ce.ChunkCount, "rows", ce.RowCount)

	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 200 * time.Millisecond
	//retryInterval := 200 * time.Millisecond
	c.execution.rowsReceived = ce.RowCount
	c.execution.chunkCount = ce.ChunkCount

	if ce.ChunkCount == 0 && ce.RowCount == 0 {
		slog.Debug("waitForConversions - no chunks/rows to write", "execution", ce.ExecutionId)
		var err error
		if ce.Error != "" {
			slog.Warn("waitForConversions - plugin execution returned error", "execution", ce.ExecutionId, "error", ce.Error)
			err = errors.New(ce.Error)
		}
		c.execution.done(err)
		return nil
	}

	// so there was no error

	err = retry.Do(ctx, retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		// check chunk count - ask the parquet writer how many chunks have been converted
		chunksWritten, err := c.parquetWriter.GetChunksWritten(ce.ExecutionId)
		if err != nil {
			return err
		}

		// if no chunks have been written, we are done
		if c.execution.chunkCount == 0 {
			slog.Warn("waitForConversions - no chunks to write", "execution", c.execution.id)
			return nil
		}

		slog.Debug("waitForConversions", "execution", c.execution.id, "chunk written", chunksWritten, "total chunks", c.execution.chunkCount)

		if chunksWritten < c.execution.chunkCount {
			slog.Debug("waiting for parquet conversion", "execution", c.execution.id, "chunks written", chunksWritten, "total chunksWritten", c.execution.chunkCount)
			// not all chunks have been written
			return retry.RetryableError(fmt.Errorf("not all chunks have been written"))
		}

		return nil
	})

	slog.Debug("waitForConversions - all chunks written", "execution", c.execution.id)

	// mark execution as complete and record the end time
	c.execution.done(err)

	// if there was an error, return it
	if err != nil {
		return err
	}

	// so we are done writing chunks - now update the db to add a view to this data
	// Open a DuckDB connection
	db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
	if err != nil {
		return err
	}
	defer db.Close()

	err = database.AddTableView(ctx, c.execution.table, db)
	if err != nil {
		return err
	}
	// notify the writer that the collection is complete
	return c.parquetWriter.JobGroupComplete(ce.ExecutionId)
}

func (c *Collector) listenToEventsAsync(ctx context.Context) {
	go func() {
		for event := range c.Events {
			c.handlePluginEvent(ctx, event)
		}
	}()
}

func (c *Collector) Compact(ctx context.Context) error {
	slog.Info("Compacting parquet files")
	c.app.Send(AwaitingCompactionMsg{})
	updateAppCompactionFunc := func(compactionStatus parquet.CompactionStatus) {
		c.app.Send(CompactionStatusUpdateMsg{status: &compactionStatus})
	}
	partitionPattern := parquet.NewPartitionPattern(c.partition)
	err := parquet.CompactDataFiles(ctx, updateAppCompactionFunc, partitionPattern)
	if err != nil {
		return fmt.Errorf("failed to compact data files: %w", err)
	}
	return nil
}

func (c *Collector) doCancel() {
	if c.cancel != nil {
		c.cancel()
	}
	// todo cleanup
}

func (c *Collector) Errors() []string {
	return c.errors
}
