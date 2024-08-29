package plugin_manager

import (
	"context"
	"fmt"
	"github.com/turbot/tailpipe/internal/constants"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/app_specific"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
)

type PluginManager struct {
	// map of running plugins, keyed by plugin name
	Plugins     map[string]*PluginClient
	pluginMutex sync.RWMutex
	// TODO #design should this be a list?
	obs        Observer
	inboxPath  string
	pluginPath string
}

func New(o Observer, inboxPath string) *PluginManager {
	return &PluginManager{
		Plugins:    make(map[string]*PluginClient),
		obs:        o,
		inboxPath:  inboxPath,
		pluginPath: filepath.Join(app_specific.InstallDir, "plugins"),
	}
}

type CollectResponse struct {
	ExecutionId     string
	PartitionSchema *schema.RowSchema
}

// Collect starts the plugin if needed, discovers the artifacts and download them for the given partition.
func (p *PluginManager) Collect(ctx context.Context, partition *config.Partition, collectionState string) (*CollectResponse, error) {
	// start plugin if needed
	plugin, err := p.getPlugin(partition.Plugin)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", partition.Plugin, err)
	}

	// TODO #design consider the flow
	// currently we create an observer for each partition, i.e. create an event stream per partition
	// perhaps instead we should have a single observer for all partitions?
	// if we keep it as it is now, it may be worth merging Collect and AddObserver and just have collect returning a stream

	// call into the plugin to collect log rows
	// this returns a stream which will send events
	// TODO #design maybe we already have an observer - or is a stream only for a single execution and reuse the stream?
	// otherwise be sure to colse the stream
	eventStream, err := plugin.AddObserver()
	if err != nil {
		return nil, fmt.Errorf("error adding observer for plugin %s: %w", plugin.Name, err)
	}
	executionID := getExecutionId()

	// tell the plugin to start the collection
	req := &proto.CollectRequest{
		ExecutionId:     executionID,
		OutputPath:      p.inboxPath,
		PartitionData:   partition.ToProto(),
		SourceData:      partition.Source.ToProto(),
		CollectionState: []byte(collectionState),
	}

	err = plugin.Collect(req)
	if err != nil {
		return nil, fmt.Errorf("error starting collection for plugin %s: %w", plugin.Name, err)
	}

	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go p.doCollect(ctx, eventStream)

	// get the schema for the partition type (we will have fetched this when starting the plugin
	partitionSchema := plugin.schemaMap[partition.Table]

	// just return - the observer is responsible for waiting for completion
	return &CollectResponse{
		ExecutionId:     executionID,
		PartitionSchema: partitionSchema,
	}, nil
}

func (p *PluginManager) Close() {
	p.pluginMutex.Lock()
	defer p.pluginMutex.Unlock()
	for _, plugin := range p.Plugins {
		plugin.client.Kill()
	}

}

// getExecutionId generates a unique id based on the current time
// this can be passed into plugin calls to assist with tracking parallel calls
func getExecutionId() string {
	// include the connection name in the call ID
	// include the connection name in the call ID
	//- it is used to identify calls to the shared cache service so there is a chance of callId clash
	return fmt.Sprintf("%d%d", time.Now().Unix(), rand.Intn(1000))
}

func (p *PluginManager) getPlugin(pluginName string) (*PluginClient, error) {
	p.pluginMutex.RLock()
	client, ok := p.Plugins[pluginName]
	p.pluginMutex.RUnlock()
	if !ok {
		p.pluginMutex.Lock()
		// recheck if plugin was started by another goroutine
		client, ok = p.Plugins[pluginName]
		if !ok {
			var err error
			client, err = p.startPlugin(pluginName)
			if err != nil {
				return nil, err
			}

		}
		p.pluginMutex.Unlock()
	}
	return client, nil
}

func (p *PluginManager) startPlugin(pluginName string) (*PluginClient, error) {
	// TODO search in dest folder for any .plugin, as steampipe does https://github.com/turbot/tailpipe/issues/4
	pluginPath := fmt.Sprintf("%s/tailpipe-plugin-%s.plugin", p.pluginPath, pluginName)
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		pluginName: &shared.TailpipeGRPCPlugin{},
	}

	pluginStartTimeout := p.getPluginStartTimeout()
	c := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          pluginMap,
		Cmd:              exec.Command("sh", "-c", pluginPath),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		// send plugin stderr (logging) to our stderr
		Stderr: os.Stderr,
		// suppress GRPC client logging
		Logger:       hclog.New(&hclog.LoggerOptions{Level: hclog.Off}),
		StartTimeout: pluginStartTimeout,
	})

	client, err := NewPluginClient(c, pluginName)
	if err != nil {
		return nil, err

	}

	// get the partition schemas for this plugin
	partitionSchemas, err := client.GetSchema()
	if err != nil {
		return nil, fmt.Errorf("error getting schema for plugin %s: %w", pluginName, err)
	}
	// store the schema for this plugin
	client.schemaMap = schema.SchemaMapFromProto(partitionSchemas.Schemas)

	// store the client
	p.Plugins[pluginName] = client

	return client, nil
}

// TODO #config #debug this is currently provided for debug purposes only
func (p *PluginManager) getPluginStartTimeout() time.Duration {
	pluginStartTimeout := 1 * time.Minute
	pluginStartTimeoutStr := os.Getenv(constants.EnvPluginStartTimeout)
	if pluginStartTimeoutStr != "" {

		t, err := strconv.Atoi(pluginStartTimeoutStr)
		if err == nil {
			pluginStartTimeout = time.Duration(t) * time.Second
		}
	}
	return pluginStartTimeout
}

func (p *PluginManager) doCollect(ctx context.Context, pluginStream proto.TailpipePlugin_AddObserverClient) {
	pluginEventChan := make(chan *proto.Event)
	errChan := make(chan error)

	// goroutine to read the plugin event stream and send the events down the event channel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- helpers.ToError(r)
			}
		}()

		for {
			e, err := pluginStream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			pluginEventChan <- e
		}
	}()

	// loop until the context is cancelled
	// TODO think about cancellation/other completion scenarios https://github.com/turbot/tailpipe/issues/8
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			if err != nil {
				// TODO #error WHAT TO DO HERE? send error to observers

				fmt.Printf("Error reading from plugin stream: %v\n", err)
				return
			}
		case protoEvent := <-pluginEventChan:
			// convert the protobuff event to an observer event
			// and send it to the observer
			if protoEvent == nil {
				// TODO #error unexpected - raise an error - send error to observers
				return
			}
			p.obs.Notify(protoEvent)
			// TODO #error should we quit if we get an error event?
			// if this is a completion event (or other error event???), stop polling
			if protoEvent.GetCompleteEvent() != nil {
				close(pluginEventChan)
				return
			}
		}
	}

}
