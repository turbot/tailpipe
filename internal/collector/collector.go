package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sethvargo/go-retry"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/events"
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
	// the execution
	execution *execution

	parquetWriter *parquet.ParquetJobPool

	// the current plugin status - used to update the spinner
	status status

	// the subdirectory of ~/.tailpipe/internal which will be used to store all file data for this collection
	// this will have the form ~/.tailpipe/internal/collection/<profile>/<pid>
	collectionTempDir string

	sourcePath string
}

func New(pluginManager *plugin_manager.PluginManager) (*Collector, error) {
	// get the temp data dir for this collection
	// - this is located  in ~/.turbot/internal/collection/<profile_name>/<pid>
	collectionTempDir := config.GlobalWorkspaceProfile.GetCollectionDir()

	c := &Collector{
		Events:            make(chan *proto.Event, eventBufferSize),
		pluginManager:     pluginManager,
		collectionTempDir: collectionTempDir,
	}

	// create a plugin manager
	c.pluginManager.AddObserver(c)

	// get the JSONL path
	sourcePath, err := sdkfilepaths.EnsureJSONLPath(c.collectionTempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSONL path: %w", err)
	}
	c.sourcePath = sourcePath

	// create bubbletea app
	//c.app = tea.NewProgram(model.NewModel(), tea.WithAltScreen())

	return c, nil
}

func (c *Collector) Close() {
	close(c.Events)

	// delete the collection temp dir
	_ = os.RemoveAll(c.collectionTempDir)
}

func (c *Collector) Collect(ctx context.Context, partition *config.Partition, fromTime time.Time) error {
	if c.execution != nil {
		return errors.New("collection already in progress")
	}

	// cleanup the collection temp dir from previous runs
	c.cleanupCollectionDir()

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, partition, fromTime, c.collectionTempDir)
	if err != nil {
		return fmt.Errorf("failed to collect: %w", err)
	}
	fmt.Printf("Collecting partition '%s' from %s (%s)\n", partition.Name(), collectResponse.FromTime.Time.Format(time.DateTime), collectResponse.FromTime.Source) //nolint:forbidigo//UI output

	executionId := collectResponse.ExecutionId
	// add the execution to the map
	c.execution = newExecution(executionId, partition)

	// create a parquet writer
	parquetWriter, err := parquet.NewParquetJobPool(executionId, partition, c.execution.conversionTiming.UpdateActiveDuration, c.sourcePath, collectResponse.Schema)
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
				rowCount, err := c.parquetWriter.GetRowCount()
				if err == nil {
					c.status.SetRowsConverted(rowCount)
					c.updateUI()
				}
			}
		}
	}()

	// start listening to plugin event
	c.listenToEventsAsync(ctx)

	return nil
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
		c.execution.state = ExecutionState_STARTED
	case *proto.Event_StatusEvent:
		c.status.UpdateWithPluginStatus(e.GetStatusEvent())
		c.updateUI()
	case *proto.Event_ChunkWrittenEvent:
		ev := e.GetChunkWrittenEvent()

		executionId := ev.ExecutionId
		chunkNumber := int(ev.ChunkNumber)

		// set the conversion start time if it hasn't been set
		c.execution.conversionTiming.TryStart(constants.TimingConvert)

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
		// TODO if no chunk written event was received, this currently stalls https://github.com/turbot/tailpipe/issues/7
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
		// set the error on the execution
		c.execution.state = ExecutionState_ERROR
		c.execution.error = fmt.Errorf("plugin error: %s", ev.Error)
	}
}

// WaitForCompletion waits for all collections to complete, then cleans up the collector - closes the file watcher
func (c *Collector) WaitForCompletion(ctx context.Context) {
	slog.Info("closing collector - wait for execution to complete")

	// wait for any ongoing partitions to complete
	err := c.waitForExecution(ctx)
	if err != nil {
		// TODO #errors x https://github.com/turbot/tailpipe/issues/106
		slog.Error("error waiting for execution to complete", "error", err)
	}

	c.parquetWriter.Close()

	// if inbox path is empty, remove it (ignore errors)
	_ = os.Remove(c.sourcePath)
}

func (c *Collector) StatusString() string {
	var str strings.Builder
	str.WriteString("Collection complete.\n\n")
	str.WriteString(c.status.String())
	str.WriteString("\n")
	// print out the execution status
	if c.execution.state == ExecutionState_ERROR {
		// if no rows were converted, just show the error
		if c.execution.totalRows == 0 {
			str.Reset()
		}
		str.WriteString(fmt.Sprintf("Execution %s failed: %s\n", c.execution.id, c.execution.error))
	}

	return str.String()
}

func (c *Collector) TimingString() string {
	var str strings.Builder
	// print out the execution status
	str.WriteString(c.execution.getTiming().String())
	str.WriteString("\n")

	return str.String()
}

// waitForConversions waits for the parquet writer to complete the conversion of the JSONL files to parquet
// it then sets the execution state to ExecutionState_COMPLETE
func (c *Collector) waitForConversions(ctx context.Context, ce *proto.EventComplete) (err error) {
	slog.Info("waiting for execution to complete", "execution", ce.ExecutionId)

	// store the plugin pluginTiming for this execution
	c.setPluginTiming(ce.ExecutionId, ce.Timing)

	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 5 * time.Second
	c.execution.totalRows = ce.RowCount
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
		// check chunk count - ask the parquet writer how many chunks have been written
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
		// so we are done writing chunks - now update the db to add a view to this data
		// Open a DuckDB connection
		db, err := sql.Open("duckdb", filepaths.TailpipeDbFilePath())
		if err != nil {
			return err
		}
		defer db.Close()

		return database.AddTableView(ctx, c.execution.table, db)
	})

	slog.Debug("waitForConversions - all chunks written", "execution", c.execution.id)

	// mark execution as complete and record the end time
	c.execution.done(err)

	// if there was an error, return it
	if err != nil {
		return err
	}

	// notify the writer that the collection is complete
	return c.parquetWriter.JobGroupComplete(ce.ExecutionId)
}

// waitForExecution waits for our execution to have state ExecutionState_COMPLETE
func (c *Collector) waitForExecution(ctx context.Context) error {
	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := executionMaxDuration
	retryInterval := 500 * time.Millisecond

	err := retry.Do(ctx, retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		switch c.execution.state {
		case ExecutionState_ERROR:
			return NewExecutionError(errors.New("execution in error state"), c.execution.id)
		case ExecutionState_COMPLETE:
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

func (c *Collector) listenToEventsAsync(ctx context.Context) {
	// TODO #control_flow do we need to consider end conditions here - check context or nil event? https://github.com/turbot/tailpipe/issues/8
	go func() {
		for event := range c.Events {
			c.handlePluginEvent(ctx, event)
		}
	}()
}

func (c *Collector) updateUI() {
	// updatye bubble teas app
	//	c.app.Update(model.NewModel(c.status.String(), c.execution.getTiming().String()))
}

func (c *Collector) setPluginTiming(executionId string, timing []*proto.Timing) {
	c.execution.pluginTiming = events.TimingCollectionFromProto(timing)
}

func (c *Collector) cleanupCollectionDir() {
	// list all folders alongside our collection temp dir
	parent := filepath.Dir(c.collectionTempDir)
	files, err := os.ReadDir(parent)
	if err != nil {
		slog.Warn("failed to list files in collection dir", "error", err)
		return
	}
	for _, file := range files {
		// if the file is a directory and is not our collection temp dir, remove it
		if file.IsDir() && file.Name() != filepath.Base(c.collectionTempDir) {
			// the folder name is the PID - check whether that pid exists
			// if it doesn't, remove the folder
			// Attempt to find the process
			// try to parse the directory name as a pid
			pid, err := strconv.ParseInt(file.Name(), 10, 32)
			if err == nil {
				if utils.PidExists(int(pid)) {
					slog.Info(fmt.Sprintf("cleanupCollectionDir skipping directory '%s' as process  with PID %d exists", file.Name(), pid))
					continue
				}
			}
			slog.Debug("removing directory", "dir", file.Name())
			_ = os.RemoveAll(filepath.Join(parent, file.Name()))
		}
	}
}
