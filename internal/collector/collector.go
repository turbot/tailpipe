package collector

import (
	"context"
	"fmt"
	"github.com/sethvargo/go-retry"
	"github.com/turbot/tailpipe/internal/paging"

	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/parquet"
	"github.com/turbot/tailpipe/internal/plugin_manager"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const eventBufferSize = 100
const parquetWorkerCount = 5

type Collector struct {
	Events chan *proto.Event

	pluginManager *plugin_manager.PluginManager
	//fileWatcher   *file_watcher.SourceFileWatcher
	// map of executions
	executions map[string]*execution
	// lock for executions
	executionsLock sync.RWMutex

	parquetWriter    *parquet.Writer
	pagingRepository *paging.Repository
}

func New(ctx context.Context) (*Collector, error) {
	// todo #config configure inbox folder https://github.com/turbot/tailpipe/issues/1
	inboxPath, err := ensureSourcePath()
	if err != nil {
		return nil, fmt.Errorf("failed to create inbox path: %w", err)
	}
	// TODO #config configure parquet output folder https://github.com/turbot/tailpipe/issues/1
	parquetPath, err := ensureDestPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet path: %w", err)
	}

	c := &Collector{
		Events:     make(chan *proto.Event, eventBufferSize),
		executions: make(map[string]*execution),
	}

	// create a plugin manager
	c.pluginManager = plugin_manager.New(c, inboxPath)

	//
	// create a parquet writer
	c.parquetWriter, err = parquet.NewWriter(inboxPath, parquetPath, parquetWorkerCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	pagingDataPath, err := ensurePagingPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create paging path: %w", err)
	}
	c.pagingRepository, err = paging.NewRepository(pagingDataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create paging repository: %w", err)
	}

	// start listening to plugin event
	c.listenToEventsAsync(ctx)
	return c, nil
}

func (c *Collector) Collect(ctx context.Context, col *config.Collection) error {
	// try to load paging data
	// TODO #config HACK everything is currently based of collection type https://github.com/turbot/tailpipe/issues/5
	pagingData, err := c.pagingRepository.Load(col.Type)
	if err != nil {
		return fmt.Errorf("failed to load paging data: %w", err)
	}

	// tell plugin to start collecting
	collectResponse, err := c.pluginManager.Collect(ctx, col, pagingData)
	if err != nil {
		return fmt.Errorf("failed to collect: %w", err)
	}

	executionId := collectResponse.ExecutionId
	// add the execution to the map
	c.executions[executionId] = &execution{
		id: executionId,
		// TODO #config for now we are just using type https://github.com/turbot/tailpipe/issues/6
		collection: col.Type,
		plugin:     col.Plugin,
		state:      ExecutionState_PENDING,
	}

	// start the parquet writer job
	return c.parquetWriter.StartCollection(executionId, col.Type, collectResponse.CollectionSchema)
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
			// TODO #errors what to do with this error?
			return
		}

		if ev.ChunkNumber%100 == 0 {
			slog.Debug("Event_ChunkWrittenEvent", "execution", ev.ExecutionId, "chunk", ev.ChunkNumber)
		}

		err := c.parquetWriter.AddChunk(executionId, chunkNumber)
		if err != nil {
			slog.Error("failed to add chunk to parquet writer", "error", err)
			// TODO #errors what to do with this error?
		}
		// store paging data
		if len(ev.PagingData) > 0 {
			err = c.pagingRepository.Save(execution.collection, string(ev.PagingData))
			if err != nil {
				slog.Error("failed to save paging data", "error", err)
				// TODO #errors what to do with this error?
			}
		}
	case *proto.Event_CompleteEvent:
		// TODO if no chunk written event was received, this currently stalls https://github.com/turbot/tailpipe/issues/7
		slog.Debug("Event_CompleteEvent", "execution", e.GetCompleteEvent().ExecutionId)

		completedEvent := e.GetCompleteEvent()
		if completedEvent.Error != "" {
			slog.Error("execution error", "execution", completedEvent.ExecutionId, "error", completedEvent.Error)
			// TODO #errors what to do with this error?
		}
		// this event means all JSON files have been written - we need to wait for all to be converted to parquet
		// we then combine the parquet files into a single file

		// start thread waiting for execution to complete
		// - this will wait for all parquet files to be written, and will then combine these into a single parquet file
		// TODO #errors what to do with an error here?
		go func() {
			err := c.waitForExecution(ctx, completedEvent)
			if err != nil {
				slog.Error("error waiting for execution to complete", "error", err)
				// TODO #errors what to do with this error?
			}
		}()

	}
}

// Close cleans up the collector - closes the file watcher
func (c *Collector) Close() {
	slog.Info("closing collector - wait for executions to complete")

	// wait for any ongoing collections to complete
	err := c.waitForExecutions()
	if err != nil {
		// TODO #errors
		slog.Error("error waiting for executions to complete", "error", err)
	}

	c.parquetWriter.Close()
	//c.fileWatcher.Close()
	c.pluginManager.Close()
	c.pagingRepository.Close()
}

func (c *Collector) waitForExecution(ctx context.Context, ce *proto.EventComplete) error {

	slog.Info("waiting for execution to complete", "execution", ce.ExecutionId)

	executionTimeout := 5 * time.Minute
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

	err := retry.Do(context.Background(), retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {
		// check chunk count - ask the parquet writer how many chunksWritten have been written
		chunksWritten, err := c.parquetWriter.GetChunksWritten(ce.ExecutionId)
		if err != nil {
			return fmt.Errorf("failed to get chunksWritten written: %w", err)
		}

		slog.Debug("waitForExecution", "execution", e.id, "chunksWritten written", chunksWritten, "total chunksWritten", e.chunkCount)

		if chunksWritten < e.chunkCount {
			slog.Debug("waiting for parquet conversion", "execution", e.id, "chunksWritten written", chunksWritten, "total chunksWritten", e.chunkCount)
			// not all chunksWritten have been written
			return retry.RetryableError(fmt.Errorf("not all chunksWritten have been written"))
		}

		return nil
	})

	if err != nil {
		e.state = ExecutionState_ERROR
		return err
	}

	slog.Debug("waitForExecution - all chunksWritten written", "execution", e.id)

	// mark execution as complete
	e.state = ExecutionState_COMPLETE
	// notify the writer that the collection is complete
	return c.parquetWriter.CollectionComplete(ce.ExecutionId)
}

func (c *Collector) waitForExecutions() error {
	// TODO #config configure timeout https://github.com/turbot/tailpipe/issues/1
	executionTimeout := 10 * time.Minute
	retryInterval := 5 * time.Second

	err := retry.Do(context.Background(), retry.WithMaxDuration(executionTimeout, retry.NewConstant(retryInterval)), func(ctx context.Context) error {

		c.executionsLock.RLock()
		defer c.executionsLock.RUnlock()
		for _, e := range c.executions {
			if e.state != ExecutionState_COMPLETE {
				//slog.Debug("waiting for executions to complete", "execution", e.id, "state", e.state)
				return retry.RetryableError(fmt.Errorf("execution %s not complete", e.id))
			}
		}

		// all complete
		return nil
	})
	if err != nil {
		return fmt.Errorf("not all executions completed after %s", executionTimeout.String())
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

func ensureSourcePath() (string, error) {
	// TODO #config configure inbox location https://github.com/turbot/tailpipe/issues/1
	sourceFilePath, err := filepath.Abs("./data/source")
	if err != nil {
		return "", fmt.Errorf("could not get absolute path for source directory: %w", err)
	}
	// ensure it exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		err = os.MkdirAll(sourceFilePath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create source directory %s: %w", sourceFilePath, err)
		}
	}

	return sourceFilePath, nil
}

func ensureDestPath() (string, error) {
	// TODO #config configure dest location https://github.com/turbot/tailpipe/issues/1
	destFilePath, err := filepath.Abs("./data/dest")
	if err != nil {
		return "", fmt.Errorf("could not get absolute path for dest directory: %w", err)
	}
	// ensure it exists
	if _, err := os.Stat(destFilePath); os.IsNotExist(err) {
		err = os.MkdirAll(destFilePath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create dest directory %s: %w", destFilePath, err)
		}
	}

	return destFilePath, nil
}

func ensurePagingPath() (string, error) {
	// TODO #config configure paging location https://github.com/turbot/tailpipe/issues/1
	destFilePath, err := filepath.Abs("./data/paging")
	if err != nil {
		return "", fmt.Errorf("could not get absolute path for dest directory: %w", err)
	}
	// ensure it exists
	if _, err := os.Stat(destFilePath); os.IsNotExist(err) {
		err = os.MkdirAll(destFilePath, 0755)
		if err != nil {
			return "", fmt.Errorf("could not create dest directory %s: %w", destFilePath, err)
		}
	}

	return destFilePath, nil
}
