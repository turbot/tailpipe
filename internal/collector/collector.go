package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/viper"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	sdkfilepaths "github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin"
)

const eventBufferSize = 100

type Collector struct {
	Events chan *proto.Event

	pluginManager *plugin.PluginManager
	// the partition to collect
	partition *config.Partition
	// the execution
	execution *execution

	parquetConvertor *parquet.Converter

	// the current plugin status - used to update the spinner
	status status
	// lock to protect the status
	statusLock sync.Mutex

	// the subdirectory of ~/.tailpipe/internal which will be used to store all file data for this collection
	// this will have the form ~/.tailpipe/internal/collection/<profile>/<pid>
	collectionTempDir string

	sourcePath string

	// bubble tea app
	app    *tea.Program
	cancel context.CancelFunc
}

func New(pluginManager *plugin.PluginManager, partition *config.Partition, cancel context.CancelFunc) (*Collector, error) {
	// get the temp data dir for this collection
	// - this is located  in ~/.turbot/internal/collection/<profile_name>/<pid>
	// first clear out any old collection temp dirs
	filepaths.CleanupCollectionTempDirs()
	collectionTempDir := filepaths.EnsureCollectionTempDir()

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

	if c.parquetConvertor != nil {
		c.parquetConvertor.Close()
	}

	// if inbox path is empty, remove it (ignore errors)
	_ = os.Remove(c.sourcePath)

	// delete the collection temp dir
	_ = os.RemoveAll(c.collectionTempDir)
}

func (c *Collector) Collect(ctx context.Context, fromTime time.Time) (err error) {
	if c.execution != nil {
		return errors.New("collection already in progress")
	}

	// create a cancel context to pass to the parquet converter
	ctx, cancel := context.WithCancel(ctx)
	// cancel the execution context if there is an error
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	// create the execution _before_ calling the plugin to ensure it is ready to receive the started event
	c.execution = newExecution(c.partition)

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, c.partition, fromTime, c.collectionTempDir)
	if err != nil {
		return err
	}

	resolvedFromTime := collectResponse.FromTime
	// now set the execution id
	c.execution.id = collectResponse.ExecutionId

	// validate the schema returned by the plugin
	err = collectResponse.Schema.Validate()
	if err != nil {
		err := fmt.Errorf("table '%s' returned invalid schema: %w", c.partition.TableName, err)
		// set execution to error
		c.execution.done(err)
		// and return error
		return err
	}

	// display the progress UI
	err = c.showCollectionStatus(resolvedFromTime)
	if err != nil {
		return err
	}

	// if there is a from time, add a filter to the partition - this will be used by the parquet writer
	if !resolvedFromTime.Time.IsZero() {
		// NOTE: handle null timestamp so we get a validation error for null timestamps, rather than excluding the row
		c.partition.AddFilter(fmt.Sprintf("(tp_timestamp is null or tp_timestamp >= '%s')", resolvedFromTime.Time.Format("2006-01-02T15:04:05")))
	}

	// create a parquet writer
	parquetConvertor, err := parquet.NewParquetConverter(ctx, cancel, c.execution.id, c.partition, c.sourcePath, collectResponse.Schema, c.updateRowCount)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	c.parquetConvertor = parquetConvertor

	// start listening to plugin event
	c.listenToEventsAsync(ctx)

	return nil
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

// WaitForCompletion waits for our execution to have state ExecutionState_COMPLETE
func (c *Collector) WaitForCompletion(ctx context.Context) error {
	if c.execution == nil {
		return errors.New("no execution in progress")
	}
	return c.execution.waitForCompletion(ctx)
}

// Compact compacts the parquet files
func (c *Collector) Compact(ctx context.Context) error {
	slog.Info("Compacting parquet files")

	c.updateApp(AwaitingCompactionMsg{})

	updateAppCompactionFunc := func(compactionStatus parquet.CompactionStatus) {
		c.statusLock.Lock()
		defer c.statusLock.Unlock()
		c.status.UpdateCompactionStatus(&compactionStatus)
		c.updateApp(CollectionStatusUpdateMsg{status: c.status})
	}
	partitionPattern := parquet.NewPartitionPattern(c.partition)
	err := parquet.CompactDataFiles(ctx, updateAppCompactionFunc, partitionPattern)
	if err != nil {
		return fmt.Errorf("failed to compact data files: %w", err)
	}
	return nil
}

// Completed marks the collection as complete and renders the summary
// - progress = true : sends completed event to tea app
// - progress = false : writes the summary to stdout
func (c *Collector) Completed() {
	c.status.complete = true
	c.updateApp(CollectionFinishedMsg{status: c.status})

	// if we suppressed progress display, we should write the summary
	if !viper.GetBool(pconstants.ArgProgress) {
		fmt.Fprint(os.Stdout, c.StatusString()) //nolint:forbidigo // we are writing to stdout
	}
}

// handlePluginEvent handles an event from a plugin
func (c *Collector) handlePluginEvent(ctx context.Context, e *proto.Event) {
	// handlePluginEvent the event
	// switch based on the struct of the event
	switch e.GetEvent().(type) {
	case *proto.Event_StartedEvent:
		slog.Info("Event_StartedEvent", "execution", e.GetStartedEvent().ExecutionId)
		c.execution.state = ExecutionState_STARTED
	case *proto.Event_StatusEvent:
		c.statusLock.Lock()
		defer c.statusLock.Unlock()
		c.status.UpdateWithPluginStatus(e.GetStatusEvent())
		c.updateApp(CollectionStatusUpdateMsg{status: c.status})
	case *proto.Event_ChunkWrittenEvent:
		ev := e.GetChunkWrittenEvent()
		executionId := ev.ExecutionId
		chunkNumber := ev.ChunkNumber

		// log every 100 chunks
		if ev.ChunkNumber%100 == 0 {
			slog.Debug("Event_ChunkWrittenEvent", "execution", ev.ExecutionId, "chunk", ev.ChunkNumber)
		}

		err := c.parquetConvertor.AddChunk(executionId, chunkNumber)
		if err != nil {
			slog.Error("failed to add chunk to parquet writer", "error", err)
			c.execution.done(err)
		}
	case *proto.Event_CompleteEvent:
		completedEvent := e.GetCompleteEvent()
		slog.Info("handlePluginEvent received Event_CompleteEvent", "execution", completedEvent.ExecutionId)

		// was there an error?
		if completedEvent.Error != "" {
			slog.Error("execution error", "execution", completedEvent.ExecutionId, "error", completedEvent.Error)
			// retrieve the execution
			c.execution.done(fmt.Errorf("plugin error: %s", completedEvent.Error))
		}
		// this event means all JSON files have been written - we need to wait for all to be converted to parquet
		// we then combine the parquet files into a single file

		// start thread waiting for conversion to complete
		// - this will wait for all parquet files to be written, and will then combine these into a single parquet file
		go func() {
			slog.Info("handlePluginEvent - waiting for conversions to complete", "execution", completedEvent.ExecutionId)
			err := c.waitForConversions(ctx, completedEvent)
			if err != nil {
				slog.Error("error waiting for execution to complete", "error", err)
				c.execution.done(err)
			} else {
				slog.Info("handlePluginEvent - conversions all complete", "execution", completedEvent.ExecutionId)
			}
		}()

	case *proto.Event_ErrorEvent:
		// TODO #errors error events are deprecated an will only be sent for plugins not using sdk > v0.2.0
		// TODO #errors decide what (if anything) we should do with error events from old plugins https://github.com/turbot/tailpipe/issues/297
		//ev := e.GetErrorEvent()
		//// for now just store errors and display at end
		////c.execution.state = ExecutionState_ERROR
		////c.execution.error = fmt.Errorf("plugin error: %s", ev.Error)
		//slog.Warn("plugin error", "execution", ev.ExecutionId, "error", ev.Error)
	}
}

func (c *Collector) createTableView(ctx context.Context) error {
	// so we are done writing chunks - now update the db to add a view to this data
	// Open a DuckDB connection
	db, err := database.NewDuckDb(database.WithDbFile(filepaths.TailpipeDbFilePath()))
	if err != nil {
		return err
	}
	defer db.Close()

	err = database.AddTableView(ctx, c.execution.table, db)
	if err != nil {
		return err
	}
	return nil
}

func (c *Collector) showCollectionStatus(resolvedFromTime *row_source.ResolvedFromTime) error {
	c.status.Init(c.partition.GetUnqualifiedName(), resolvedFromTime)

	if viper.GetBool(pconstants.ArgProgress) {
		return c.showTeaAppAsync()
	}

	return c.showMinimalCollectionStatus()
}

func (c *Collector) showTeaAppAsync() error {
	c.app = tea.NewProgram(newCollectionModel(c.status))

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
	return nil
}

func (c *Collector) showMinimalCollectionStatus() error {
	// display initial message
	initMsg := c.status.CollectionHeader()
	_, err := fmt.Print(initMsg) //nolint:forbidigo //desired output
	return err
}

// updateRowCount is called directly by the parquet writer to update the row count
func (c *Collector) updateRowCount(rowCount, errorCount int64, errors ...error) {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()

	c.status.UpdateConversionStatus(rowCount, errorCount, errors...)

	c.updateApp(CollectionStatusUpdateMsg{status: c.status})
}

func (c *Collector) StatusString() string {
	var str strings.Builder
	str.WriteString(c.status.String())
	str.WriteString("\n")
	// print out the execution status
	if c.execution.state == ExecutionState_ERROR {
		// if no rows were converted, just show the error
		if c.status.RowsReceived == 0 {
			str.Reset()
		}
		str.WriteString(fmt.Sprintf("Execution failed: %s\n", c.execution.error))
	}

	return str.String()
}

// waitForConversions waits for the parquet writer to complete the conversion of the JSONL files to parquet
// it then sets the execution state to ExecutionState_COMPLETE
func (c *Collector) waitForConversions(ctx context.Context, ce *proto.EventComplete) (err error) {
	slog.Info("waitForConversions - waiting for execution to complete", "execution", ce.ExecutionId, "chunks", ce.ChunkCount, "rows", ce.RowCount)

	if ce.ChunkCount == 0 && ce.RowCount == 0 {
		slog.Debug("waitForConversions - no chunks/rows to write", "execution", ce.ExecutionId)
		var err error
		if ce.Error != "" {
			slog.Warn("waitForConversions - plugin execution returned error", "execution", ce.ExecutionId, "error", ce.Error)
			err = errors.New(ce.Error)
		}
		// mark execution as done, passing any error
		c.execution.done(err)
		return nil
	}

	// so there was no plugin error - wait for the conversions to complete
	c.parquetConvertor.WaitForConversions(ctx)

	if err := c.createTableView(ctx); err != nil {
		slog.Error("error creating table view", "error", err)
		c.execution.done(err)
		return err
	}

	// mark execution as complete and record the end time
	c.execution.done(err)

	// if there was an error, return it
	if err != nil {
		return err
	}

	// notify the writer that the collection is complete
	return nil
}

func (c *Collector) listenToEventsAsync(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-c.Events:
				c.handlePluginEvent(ctx, event)
			}
		}
	}()
}

func (c *Collector) doCancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Collector) updateApp(msg tea.Msg) {
	if c.app != nil {
		c.app.Send(msg)
	}
}
