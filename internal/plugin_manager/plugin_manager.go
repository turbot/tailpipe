package plugin_manager

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/go-version"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/installationstate"
	pociinstaller "github.com/turbot/pipe-fittings/v2/ociinstaller"
	pplugin "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/statushooks"
	"github.com/turbot/tailpipe-plugin-core/core"
	"github.com/turbot/tailpipe-plugin-sdk/grpc"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/ociinstaller"
	"github.com/turbot/tailpipe/internal/plugin"
	"google.golang.org/protobuf/types/known/timestamppb"

	// refer to artifact source so sdk sources are registered
	_ "github.com/turbot/tailpipe-plugin-sdk/artifact_source"
)

type PluginManager struct {
	// map of running plugins, keyed by plugin name
	Plugins     map[string]*grpc.PluginClient
	pluginMutex sync.RWMutex
	obs         Observer
	pluginPath  string
}

func New() *PluginManager {
	return &PluginManager{
		Plugins:    make(map[string]*grpc.PluginClient),
		pluginPath: filepath.Join(app_specific.InstallDir, "plugins"),
	}
}

// AddObserver adds a
// n observer to the plugin manager
func (p *PluginManager) AddObserver(o Observer) {
	p.obs = o
}

// Collect starts the plugin if needed, discovers the artifacts and download them for the given partition.
func (p *PluginManager) Collect(ctx context.Context, partition *config.Partition, fromTime time.Time, collectionTempDir string) (*CollectResponse, error) {
	// start plugin if needed
	tablePlugin := partition.Plugin
	tablePluginClient, err := p.getPlugin(ctx, tablePlugin)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", partition.Plugin.Alias, err)
	}

	var sourcePluginReattach *proto.SourcePluginReattach
	// identify which plugin provides the source
	sourcePlugin, err := p.determineSourcePlugin(partition)
	if err != nil {
		return nil, fmt.Errorf("error determining source plugin for source %s: %w", partition.Source.Type, err)
	}
	// if this plugin is different from the plugin that provides the table, we need to start the source plugin,
	// and then pass reattach info
	if sourcePlugin.Plugin != tablePlugin.Plugin {
		sourcePluginClient, err := p.getPlugin(ctx, sourcePlugin)
		if err != nil {
			return nil, fmt.Errorf("error starting plugin '%s' required for source '%s': %w", sourcePlugin.Alias, partition.Source.Type, err)
		}
		sourcePluginReattach = proto.NewSourcePluginReattach(partition.Source.Type, sourcePlugin.Alias, sourcePluginClient.Client.ReattachConfig())
	}

	// call into the plugin to collect log rows
	// this returns a stream which will send events
	// be sure to close the stream
	eventStream, err := tablePluginClient.AddObserver()
	if err != nil {
		return nil, fmt.Errorf("error adding observer for plugin %s: %w", tablePluginClient.Name, err)
	}
	executionID := getExecutionId()

	// the collection temp dir is a subfolder of the collection dir, which has
	// with the form: ~/.tailpipe/collection/<profile_name>/<pid>/
	// it is used to write JSONL files and temporary source artifact files
	// it is expected all this data will be cleaned up after the collection is complete
	// these folders are subject to cleanup in the future

	// the plugin collection state is written by the plugin to a json file in the parent of the collection temp dir:
	// : ~/.tailpipe/collection/<profile_name>/
	// the name of the collection state file contains the partition name
	// thus the collection state is shared between multiple successive collections

	// build the collection state path
	collectionStatePath := partition.CollectionStatePath(config.GlobalWorkspaceProfile.GetCollectionDir())

	// tell the plugin to start the collection
	req := &proto.CollectRequest{
		TableName:           partition.TableName,
		PartitionName:       partition.ShortName,
		ExecutionId:         executionID,
		CollectionTempDir:   collectionTempDir,
		CollectionStatePath: collectionStatePath,
		SourceData:          partition.Source.ToProto(),
		SourcePlugin:        sourcePluginReattach,
		FromTime:            timestamppb.New(fromTime),
	}

	if partition.Source.Connection != nil {
		req.ConnectionData = partition.Source.Connection.ToProto()
	}

	if partition.Source.Format != nil {
		req.SourceFormat = partition.Source.Format.ToProto()
	}
	if partition.CustomTable != nil {
		req.CustomTableSchema = partition.CustomTable.ToProto()
		// set the default source format if the source dow not provide one
		if req.SourceFormat == nil && partition.CustomTable.DefaultSourceFormat != nil {
			req.SourceFormat = partition.CustomTable.DefaultSourceFormat.ToProto()
		}
		if req.SourceFormat == nil {
			return nil, fmt.Errorf("no source format defined for custom table %s", partition.CustomTable.ShortName)
		}
	}
	if sourcePluginReattach != nil {
		req.SourcePlugin = sourcePluginReattach
	}

	collectResponse, err := tablePluginClient.Collect(req)
	if err != nil {
		return nil, fmt.Errorf("error starting collection for plugin %s: %w", tablePluginClient.Name, error_helpers.TransformErrorToSteampipe(err))
	}

	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go p.readCollectionEvents(ctx, eventStream)

	// just return - the observer is responsible for waiting for completion
	return CollectResponseFromProto(collectResponse), nil
}

func (p *PluginManager) UpdateCollectionState(ctx context.Context, partition *config.Partition, fromTime time.Time, collectionStatePath string) error {
	// identify which plugin provides the source
	sourcePlugin, err := p.determineSourcePlugin(partition)
	if err != nil {
		return fmt.Errorf("error determining source plugin for source %s: %w", partition.Source.Type, err)
	}

	// start plugin if needed
	pluginClient, err := p.getPlugin(ctx, sourcePlugin)
	if err != nil {
		return fmt.Errorf("error starting plugin %s: %w", partition.Plugin.Alias, err)
	}

	// reuse CollectRequest for UpdateCollectionState
	req := &proto.UpdateCollectionStateRequest{
		CollectionStatePath: collectionStatePath,
		SourceData:          partition.Source.ToProto(),
		FromTime:            timestamppb.New(fromTime),
	}

	_, err = pluginClient.UpdateCollectionState(req)
	if err != nil {
		return fmt.Errorf("error updating collection state for plugin %s: %w", pluginClient.Name, error_helpers.TransformErrorToSteampipe(err))
	}

	// just return - the observer is responsible for waiting for completion
	return err
}

// Describe starts the plugin if needed, discovers the artifacts and download them for the given partition.
func (p *PluginManager) Describe(ctx context.Context, pluginName string) (*PluginDescribeResponse, error) {
	// build plugin ref from the name
	pluginDef := pplugin.NewPlugin(pluginName)

	pluginClient, err := p.getPlugin(ctx, pluginDef)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", pluginDef.Alias, err)
	}

	describeResponse, err := pluginClient.Describe()
	if err != nil {
		return nil, fmt.Errorf("error starting describeion for plugin %s: %w", pluginClient.Name, err)
	}

	res := DescribeResponseFromProto(describeResponse)
	res.Name = pluginDef.Plugin

	// just return - the observer is responsible for waiting for completion
	return res, nil
}

func (p *PluginManager) Close() {
	p.pluginMutex.Lock()
	defer p.pluginMutex.Unlock()
	for _, plg := range p.Plugins {
		plg.Client.Kill()
	}
}

// getExecutionId generates a unique id based on the current time
// this can be passed into plugin calls to assist with tracking parallel calls
func getExecutionId() string {
	return fmt.Sprintf("%d%d", time.Now().Unix(), rand.Int32N(1000)) //nolint:gosec// strong enough for the execution id
}

func (p *PluginManager) getPlugin(ctx context.Context, pluginDef *pplugin.Plugin) (*grpc.PluginClient, error) {
	if pluginDef.Alias == constants.CorePluginName {
		// ensure the core plugin is installed or the min version requirement is satisfied
		if err := ensureCorePlugin(ctx); err != nil {
			return nil, err
		}
	}

	p.pluginMutex.RLock()
	// Plugins map is keyed by image ref
	pluginImageRef := pluginDef.Plugin
	client, ok := p.Plugins[pluginImageRef]
	p.pluginMutex.RUnlock()
	if !ok {
		p.pluginMutex.Lock()
		defer p.pluginMutex.Unlock()
		// recheck if pluginImageRef was started by another goroutine
		client, ok = p.Plugins[pluginImageRef]
		if !ok {
			var err error
			client, err = p.startPlugin(pluginDef)
			if err != nil {
				return nil, err
			}
		}
	}
	return client, nil
}

func (p *PluginManager) startPlugin(tp *pplugin.Plugin) (*grpc.PluginClient, error) {
	pluginName := tp.Alias

	pluginPath, err := pfilepaths.GetPluginPath(tp.Plugin, tp.Alias)
	if err != nil {
		return nil, fmt.Errorf("error getting plugin path for plugin '%s': %w", tp.Alias, err)
	}

	// create the plugin map
	pluginMap := map[string]goplugin.Plugin{
		pluginName: &shared.TailpipeGRPCPlugin{},
	}

	pluginStartTimeout := p.getPluginStartTimeout()
	c := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          pluginMap,
		Cmd:              exec.Command("sh", "-c", pluginPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
		// send plugin stderr (logging) to our stderr
		Stderr: os.Stderr,
		// suppress GRPC client logging
		Logger:       hclog.New(&hclog.LoggerOptions{Level: hclog.Off}),
		StartTimeout: pluginStartTimeout,
	})

	client, err := grpc.NewPluginClient(c, pluginName)
	if err != nil {
		return nil, err

	}

	// store the client, keyed by image ref
	p.Plugins[tp.Plugin] = client

	return client, nil
}

// for debug purposes, plugin start timeout can be set via an environment variable TAILPIPE_PLUGIN_START_TIMEOUT
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

func (p *PluginManager) readCollectionEvents(ctx context.Context, pluginStream proto.TailpipePlugin_AddObserverClient) {
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
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			if err != nil {
				// TODO #error handle error
				// ignore EOF errors
				if !strings.Contains(err.Error(), "EOF") {
					fmt.Printf("Error reading from plugin stream: %s\n", err.Error()) //nolint:forbidigo// TODO #error
				}
				return
			}
		case protoEvent := <-pluginEventChan:
			// convert the protobuf event to an observer event
			// and send it to the observer
			if protoEvent == nil {
				// TODO #error unexpected - raise an error - send error to observers
				return
			}
			p.obs.Notify(protoEvent)
			// TODO #error should we stop polling if we get an error event?
			// if this is a completion event (or other error event???), stop polling
			if protoEvent.GetCompleteEvent() != nil {
				close(pluginEventChan)
				return
			}
		}
	}

}

func (p *PluginManager) determineSourcePlugin(partition *config.Partition) (*pplugin.Plugin, error) {
	sourceType := partition.Source.Type
	// because we reference the core plugin, all sources it provides are registered with our source factory instance
	coreSources, err := core.DescribeSources()
	if err != nil {
		return nil, fmt.Errorf("error describing sources: %w", err)
	}
	if _, ok := coreSources[sourceType]; ok {
		return pplugin.NewPlugin(constants.CorePluginName), nil
	}

	// assume the source type name is of form "<plugin>_<source>", eg. aws_s3_bucket -> "aws"
	pluginName := strings.Split(sourceType, "_")[0]
	return pplugin.NewPlugin(pluginName), nil
}

// ensureCorePlugin ensures the core plugin is installed or the min version is satisfied
func ensureCorePlugin(ctx context.Context) error {
	// get the installation state
	state, err := installationstate.Load()
	if err != nil {
		return err
	}

	// check if core plugin is already installed
	exists, _ := pplugin.Exists(ctx, constants.CorePluginName)

	if exists {
		// check if the min version is satisfied; if not then update
		// retrieve the plugin version data from tailpipe config
		pluginVersions := config.GlobalConfig.PluginVersions
		// find the version of the core plugin from the pluginVersions
		installedVersion := pluginVersions[constants.CorePluginFullName].Version

		// compare the version(using semver) with the min version
		satisfy, err := checkSatisfyMinVersion(installedVersion)
		if err != nil {
			return err
		}
		if !satisfy {
			// install the core plugin
			if err = installCorePlugin(ctx, state, "Updating"); err != nil {
				return err
			}
		}

	} else {
		// install the core plugin
		if err = installCorePlugin(ctx, state, "Installing"); err != nil {
			return err
		}
	}
	return nil
}

func installCorePlugin(ctx context.Context, state installationstate.InstallationState, operation string) error {
	spinner := statushooks.NewStatusSpinnerHook()
	spinner.Show()
	defer spinner.Hide()
	spinner.SetStatus(fmt.Sprintf("%s core plugin", operation))

	// get the latest version of the core plugin
	ref := pociinstaller.NewImageRef(constants.CorePluginName)
	org, name, constraint := ref.GetOrgNameAndStream()
	rpv, err := pplugin.GetLatestPluginVersionByConstraint(ctx, state.InstallationID, org, name, constraint)
	if err != nil {
		return err
	}
	resolvedPlugin := *rpv

	progress := make(chan struct{}, 5)

	// install plugin
	_, err = plugin.Install(ctx, resolvedPlugin, progress, constants.BaseImageRef, ociinstaller.TailpipeMediaTypeProvider{}, pociinstaller.WithSkipConfig(viper.GetBool(pconstants.ArgSkipConfig)))
	if err != nil {
		return err
	}
	return nil
}

func checkSatisfyMinVersion(ver string) (bool, error) {
	// check if the version satisfies the min version requirement of core plugin
	// Parse the versions
	installedVer, err := version.NewVersion(ver)
	if err != nil {
		return false, err
	}
	minReq, err := version.NewVersion(constants.MinCorePluginVersion)
	if err != nil {
		return false, err
	}

	// compare the versions
	if installedVer.LessThan(minReq) {
		return false, nil
	}
	return true, nil
}
