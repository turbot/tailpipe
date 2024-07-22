package plugin_manager

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
	"github.com/turbot/tailpipe/internal/config"
)

// todo configure this
const pluginLocation = "/Users/kai/.tailpipe/plugins"

type PluginManager struct {

	// map of running plugins, keyed by plugin name
	Plugins     map[string]*PluginClient
	pluginMutex sync.RWMutex
	// TODO should this be a list
	obs       Observer
	inboxPath string
}

func New(o Observer, inboxPath string) *PluginManager {
	return &PluginManager{
		//observations: make(map[string]*observer.Observable),
		Plugins:   make(map[string]*PluginClient),
		obs:       o,
		inboxPath: inboxPath,
	}
}

type CollectResponse struct {
	ExecutionId      string
	CollectionSchema *schema.RowSchema
}

// Collect starts the plugin if needed, discovers the artifacts and download them for the given collection.
func (p *PluginManager) Collect(ctx context.Context, collection *config.Collection) (*CollectResponse, error) {
	// start plugin if needed
	plugin, err := p.getPlugin(collection.Plugin)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", collection.Plugin, err)
	}

	// TODO consider the flow
	// currently we create an observer for each collection, i.e. create an event stream per collection
	// perhaps instead we should have a single observer for all collections?
	// if we keep it as it is now, it may be worth merging Colection and AddObserver and just have collect returning a stream

	// call into the plugin to collect log rows
	// this returns a stream which will send events
	// TODO maybe we already have an observer - or is a stream only for a single execution and reuse the stream?
	// otherwise be sure to colse the stream
	eventStream, err := plugin.AddObserver()
	if err != nil {
		return nil, fmt.Errorf("error adding observer for plugin %s: %w", plugin.Name, err)
	}
	executionID := getExecutionId()

	// tell the plugin to start the collection
	req := &proto.CollectRequest{
		ExecutionId:    executionID,
		OutputPath:     p.inboxPath,
		CollectionName: collection.Type,
		Config:         collection.Config,
	}

	err = plugin.Collect(req)
	if err != nil {
		return nil, fmt.Errorf("error starting collection for plugin %s: %w", plugin.Name, err)
	}

	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go p.doCollect(ctx, eventStream)

	// get the schema for the collection (we will have fetched this when starting the plugin
	collectionSchema := plugin.schemaMap[collection.Type]

	// just return - the observer is responsible for waiting for completion
	return &CollectResponse{
		ExecutionId:      executionID,
		CollectionSchema: collectionSchema,
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
	// TODO sort out plugin location/name conventions, i.e. alias vs full name/identifer etc
	// todo search in dest folder for any .plugin, as steampipe does
	pluginPath := fmt.Sprintf("%s/tailpipe-plugin-%s.plugin", pluginLocation, pluginName)
	// create the plugin map
	pluginMap := map[string]plugin.Plugin{
		pluginName: &shared.TailpipeGRPCPlugin{},
	}

	c := plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          pluginMap,
		Cmd:              exec.Command("sh", "-c", pluginPath),
		AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC},
		// send plugin stderr (logging) to our stderr
		Stderr: os.Stderr,
		// suppress GRPC client logging
		Logger: hclog.New(&hclog.LoggerOptions{Level: hclog.Off}),
	})

	client, err := NewPluginClient(c, pluginName)
	if err != nil {
		return nil, err

	}

	// get the collection schemas for this plugin
	collectionSchemas, err := client.GetSchema()
	if err != nil {
		return nil, fmt.Errorf("error getting schema for plugin %s: %w", pluginName, err)
	}
	// store the schema for this plugin
	client.schemaMap = schema.SchemaMapFromProto(collectionSchemas.Schemas)

	// store the client
	p.Plugins[pluginName] = client

	return client, nil
}

func (p *PluginManager) doCollect(ctx context.Context, pluginStream proto.TailpipePlugin_AddObserverClient) {
	pluginEventChan := make(chan *proto.Event)
	errChan := make(chan error)

	// goroutine to read the plugin event stream
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
	// TODO think about cancellation/other completion scenarios
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			if err != nil {
				// TODO WHAT TO DO HERE? send error to observers

				fmt.Printf("Error reading from plugin stream: %v\n", err)
				return
			}
		case protoEvent := <-pluginEventChan:
			// convert the protobuff event to an observer event
			// and send it to the observer
			if protoEvent == nil {
				// TODO unexpected - raise an error - send error to observers
				return
			}
			p.obs.Notify(protoEvent)
			// if this is a completion event (or other error event???), stop polling
			if protoEvent.GetCompleteEvent() != nil {
				close(pluginEventChan)
				return
			}

		}
	}

}
