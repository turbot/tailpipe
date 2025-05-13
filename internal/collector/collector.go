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
	"github.com/turbot/tailpipe-plugin-sdk/events"
	sdkfilepaths "github.com/turbot/tailpipe-plugin-sdk/filepaths"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin"
)

const eventBufferSize = 100

type Collector struct {
	// this buffered channel is used to asynchronously process events from the plugin
	// our Notify function will send events to this channel
	Events chan events.Event

	// the plugin manager is responsible for managing the plugin lifecycle and brokering
	// communication between the plugin and the collector
	pluginManager *plugin.PluginManager
	// the partition to collect
	partition *config.Partition
	// the execution is used to manage the state of the collection
	execution *execution
	// the parquet convertor is used to convert the JSONL files to parquet
	parquetConvertor *parquet.Converter

	// the current plugin status - used to update the spinner
	status status
	// lock to protect the status
	statusLock sync.Mutex

	// the subdirectory of ~/.tailpipe/internal which will be used to store all file data for this collection
	// this will have the form ~/.tailpipe/internal/collection/<profile>/<pid>
	collectionTempDir string
	// the path to the JSONL files - the plugin will write to this path
	sourcePath string

	// bubble tea app
	app    *tea.Program
	cancel context.CancelFunc
}

// New creates a new collector
func New(pluginManager *plugin.PluginManager, partition *config.Partition, cancel context.CancelFunc) (*Collector, error) {
	// get the temp data dir for this collection
	// - this is located  in ~/.turbot/internal/collection/<profile_name>/<pid>
	// first clear out any old collection temp dirs
	filepaths.CleanupCollectionTempDirs()
	// then create a new collection temp dir
	collectionTempDir := filepaths.EnsureCollectionTempDir()

	// create the collector
	c := &Collector{
		Events:            make(chan events.Event, eventBufferSize),
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

// Close closes the collector
// - closes the events channel
// - closes the parquet convertor
// - removes the JSONL path
// - removes the collection temp dir
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

// Collect asynchronously starts the collection process
// - creates a new execution
// - tells the plugin manager to start collecting
// - validates the schema returned by the plugin
// - starts the collection UI
// - creates a parquet writer, which will process the JSONL files as they are written
// - starts listening to plugin events
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

	// create the execution
	// NOTE: create _before_ calling the plugin to ensure it is ready to receive the started event
	c.execution = newExecution(c.partition)

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, c.partition, fromTime, c.collectionTempDir)
	if err != nil {
		return err
	}

	// _now_ set the execution id
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
	// determine the time to start collecting from
	resolvedFromTime := collectResponse.FromTime

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

	// start listening to plugin events asynchronously
	go c.listenToEvents(ctx)

	return nil
}

// Notify implements observer.Observer
// send an event down the channel to be picked up by the handlePluginEvent goroutine
func (c *Collector) Notify(_ context.Context, event events.Event) error {
	// only send the event if the execution is not complete - this is to handle the case where it has
	// terminated with an error, causing the collector to close, closing the channel
	if !c.execution.complete() {
		c.Events <- event
	}
	return nil
}

// WaitForCompletion waits for our execution to have state ExecutionState_COMPLETE
func (c *Collector) WaitForCompletion(ctx context.Context) error {
	if c.execution == nil {
		return errors.New("no execution in progress")
	}
	return c.execution.waitForCompletion(ctx)
}

// Compact compacts the parquet files so that for each date folder
// (the lowest level of the partition) there is only one parquet file
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
		_, _ = fmt.Fprint(os.Stdout, c.StatusString()) //nolint:forbidigo // UI output
	}
}

// handlePluginEvent handles an event from a plugin
func (c *Collector) handlePluginEvent(ctx context.Context, e events.Event) {
	// handlePluginEvent the event
	// switch based on the struct of the event
	switch ev := e.(type) {
	case *events.Started:
		slog.Info("Started event", "execution", ev.ExecutionId)
		c.execution.state = ExecutionState_STARTED
	case *events.Status:
		c.statusLock.Lock()
		defer c.statusLock.Unlock()
		c.status.UpdateWithPluginStatus(ev)
		c.updateApp(CollectionStatusUpdateMsg{status: c.status})
	case *events.Chunk:

		executionId := ev.ExecutionId
		chunkNumber := ev.ChunkNumber

		// log every 100 chunks
		if ev.ChunkNumber%100 == 0 {
			slog.Debug("Chunk event", "execution", ev.ExecutionId, "chunk", ev.ChunkNumber)
		}

		err := c.parquetConvertor.AddChunk(executionId, chunkNumber)
		if err != nil {
			slog.Error("failed to add chunk to parquet writer", "error", err)
			c.execution.done(err)
		}
	case *events.Complete:
		slog.Info("Complete event", "execution", ev.ExecutionId)

		// was there an error?
		if ev.Err != nil {
			slog.Error("execution error", "execution", ev.ExecutionId, "error", ev.Err)
			// update the execution
			c.execution.done(ev.Err)
			return
		}
		// this event means all JSON files have been written - we need to wait for all to be converted to parquet
		// we then combine the parquet files into a single file

		// start thread waiting for conversion to complete
		// - this will wait for all parquet files to be written, and will then combine these into a single parquet file
		slog.Info("handlePluginEvent - waiting for conversions to complete")
		go func() {
			err := c.waitForConversions(ctx, ev)
			if err != nil {
				slog.Error("error waiting for execution to complete", "error", err)
				c.execution.done(err)
			} else {
				slog.Info("handlePluginEvent - conversions all complete")
			}
		}()

	case *events.Error:
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

	// if the progress flag is set, start the tea app to display the progress
	if viper.GetBool(pconstants.ArgProgress) {
		// start the tea app asynchronously
		go c.showTeaApp()
		return nil
	}
	// otherwise, just show a simple initial message - we will show a simple summary tat the end
	return c.showMinimalCollectionStartMessage()
}

// showTeaApp starts the tea app to display the collection progress
func (c *Collector) showTeaApp() {
	// create the tea app
	c.app = tea.NewProgram(newCollectionModel(c.status))
	// and start it
	model, err := c.app.Run()
	if model.(collectionModel).cancelled {
		slog.Info("Collection UI returned cancelled")
		c.doCancel()
	}
	if err != nil {
		slog.Warn("Collection UI returned error", "error", err)
		c.doCancel()
	}
}

// showMinimalCollectionStartMessage displays a simple status message to indicate that the collection has started
// this is used when the progress flag is not set
func (c *Collector) showMinimalCollectionStartMessage() error {
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
func (c *Collector) waitForConversions(ctx context.Context, ce *events.Complete) (err error) {
	slog.Info("waitForConversions - waiting for execution to complete", "execution", ce.ExecutionId, "chunks", ce.ChunksWritten, "rows", ce.RowCount)

	// ensure we mark the execution as done
	defer func() {
		c.execution.done(err)
	}()

	// if there are no chunks or rows to write, we can just return
	// (note: we know that the Complete event wil not have an error as that is handled before calling this function)
	if ce.ChunksWritten == 0 && ce.RowCount == 0 {
		slog.Debug("waitForConversions - no chunks/rows to write", "execution", ce.ExecutionId)
		return nil
	}

	// wait for the conversions to complete
	c.parquetConvertor.WaitForConversions(ctx)

	// create or update the table view for ths table being collected
	if err := c.createTableView(ctx); err != nil {
		slog.Error("error creating table view", "error", err)
		return err
	}

	slog.Info("handlePluginEvent - conversions all complete")

	return nil
}

// listenToEvents listens to the events channel and handles events
func (c *Collector) listenToEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-c.Events:
			c.handlePluginEvent(ctx, event)
		}
	}
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
