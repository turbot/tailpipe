package plugin

import (
	"context"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	goplugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/go-version"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/spf13/viper"
	gokithelpers "github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/installationstate"
	pociinstaller "github.com/turbot/pipe-fittings/v2/ociinstaller"
	pplugin "github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/statushooks"
	"github.com/turbot/pipe-fittings/v2/versionfile"
	"github.com/turbot/tailpipe-plugin-core/core"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/observable"
	"github.com/turbot/tailpipe-plugin-sdk/types"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/helpers"
	"github.com/turbot/tailpipe/internal/ociinstaller"
	"google.golang.org/protobuf/types/known/timestamppb"

	// refer to artifact source so sdk sources are registered
	_ "github.com/turbot/tailpipe-plugin-sdk/artifact_source"
)

type PluginManager struct {
	// map of running plugins, keyed by plugin name
	Plugins     map[string]*grpc.PluginClient
	pluginMutex sync.RWMutex
	// the observer to notify of events
	// (this will be the collector)
	obs        observable.Observer
	pluginPath string
}

func NewPluginManager() *PluginManager {
	return &PluginManager{
		Plugins:    make(map[string]*grpc.PluginClient),
		pluginPath: filepath.Join(app_specific.InstallDir, "plugins"),
	}
}

// AddObserver adds a
// n observer to the plugin manager
func (p *PluginManager) AddObserver(o observable.Observer) {
	p.obs = o
}

// Collect starts the plugin if needed, discovers the artifacts and download them for the given partition.
func (p *PluginManager) Collect(ctx context.Context, partition *config.Partition, fromTime time.Time, toTime time.Time, recollect bool, collectionTempDir string) (*CollectResponse, error) {
	// start plugin if needed
	tablePlugin := partition.Plugin
	tablePluginClient, err := p.getPlugin(tablePlugin)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", partition.Plugin.Alias, err)
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
		FromTime:            timestamppb.New(fromTime),
		ToTime:              timestamppb.New(toTime),
		TempDirMaxMb:        viper.GetInt64(pconstants.ArgTempDirMaxMb),
		Recollect:           recollect,
	}

	if partition.Source.Connection != nil {
		req.ConnectionData = partition.Source.Connection.ToProto()
	}

	// identify which plugin provides the source and if it is different from the table plugin,
	// we need to start the source plugin, and then pass reattach info
	sourcePluginClient, sourcePluginReattach, err := p.getSourcePluginReattach(ctx, partition, tablePlugin)
	if err != nil {
		return nil, err
	}
	// set on req (may be nil - this is fine)
	req.SourcePlugin = sourcePluginReattach

	err = p.verifySupportedOperations(tablePluginClient, sourcePluginClient, toTime)
	if err != nil {
		return nil, err
	}

	// start a goroutine to monitor the plugins
	// populate the custom table
	if partition.CustomTable != nil {
		req.CustomTableSchema = partition.CustomTable.ToProto()
	}

	// now populate the format if necessary
	if format := partition.GetFormat(); format != nil {
		// populate the format data to pass to the plugin
		pf, err := p.formatToProto(ctx, format, tablePlugin)
		if err != nil {
			return nil, err
		}
		req.SourceFormat = pf
	}

	collectResponse, err := tablePluginClient.Collect(req)
	if err != nil {
		return nil, fmt.Errorf("error starting collection for plugin %s: %w", tablePluginClient.Name, error_helpers.TransformErrorToSteampipe(err))
	}

	// start a goroutine to read the eventStream and listen to file events
	// this will loop until it hits an error or the stream is closed
	go p.readCollectionEvents(ctx, executionID, eventStream)

	// just return - the observer is responsible for waiting for completion
	return CollectResponseFromProto(collectResponse), nil
}

func (p *PluginManager) verifySupportedOperations(tablePluginClient *grpc.PluginClient, sourcePluginClient *grpc.PluginClient, toTime time.Time) error {
	tablePluginSupportedOperations, err := p.getSupportedOperations(tablePluginClient)
	var sourcePluginSupportedOperations *proto.GetSupportedOperationsResponse
	if err != nil {
		return fmt.Errorf("error getting supported operations for plugin %s: %w", tablePluginClient.Name, err)
	}
	if sourcePluginClient != nil {
		sourcePluginSupportedOperations, err = p.getSupportedOperations(tablePluginClient)
		if err != nil {
			return fmt.Errorf("error getting supported operations for plugin %s: %w", tablePluginClient.Name, err)
		}
	}

	// if the plugin does not support time ranges:
	// - we cannot specify a 'To' time
	// - hard code recollect to true - this is the default behaviour for plugins that do not support time ranges
	// if a 'To' time' is set, we must ensure the plugin supports time ranges
	if !tablePluginSupportedOperations.TimeRanges {
		// get friendly plugin name
		ref := pociinstaller.NewImageRef(tablePluginClient.Name)
		pluginName := ref.GetFriendlyName()

		//if !toTime.IsZero() {
		//	return fmt.Errorf("plugin '%s' does not support specifying a 'To' time - try updating the plugin", pluginName)
		//}
		slog.Info("plugin does not support time ranges - setting 'Recollect' to true", "plugin", pluginName)
		viper.Set(pconstants.ArgRecollect, true)
	}

	if sourcePluginSupportedOperations != nil && !sourcePluginSupportedOperations.TimeRanges {
		// get friendly plugin name
		ref := pociinstaller.NewImageRef(sourcePluginClient.Name)
		pluginName := ref.GetFriendlyName()

		if !toTime.IsZero() {
			return fmt.Errorf("source plugin '%s' does not support specifying a 'To' time - try updating the plugin", pluginName)
		}
		slog.Info("source plugin does not support time ranges - setting 'Recollect' to true", "plugin", pluginName)
		viper.Set(pconstants.ArgRecollect, true)
	}
	return nil
}

func (p *PluginManager) getSupportedOperations(tablePluginClient *grpc.PluginClient) (*proto.GetSupportedOperationsResponse, error) {
	supportedOperations, err := tablePluginClient.GetSupportedOperations()
	if err != nil {
		// if the plugin does not implement GetSupportedOperations, it will return a NotImplemented error
		// just return an empty response
		if helpers.IsNotGRPCImplementedError(err) {
			return &proto.GetSupportedOperationsResponse{}, nil
		}
	}
	return supportedOperations, err
}

// Describe starts the plugin if needed, and returns the plugin description, including description of any custom formats
func (p *PluginManager) Describe(ctx context.Context, pluginName string, opts ...DescribeOpts) (*types.DescribeResponse, error) {
	// build plugin ref from the name
	pluginDef := pplugin.NewPlugin(pluginName)

	pluginClient, err := p.getPlugin(pluginDef)
	if err != nil {
		return nil, fmt.Errorf("error starting plugin %s: %w", pluginDef.Alias, err)
	}

	req := &proto.DescribeRequest{}
	// apply opts - these may set the custom formats and custom formats only flag
	for _, opt := range opts {
		opt(req)
	}

	describeResponse, err := pluginClient.Describe(req)
	if err != nil {
		return nil, fmt.Errorf("error calling describe for plugin %s: %w", pluginClient.Name, err)
	}

	// build DescribeResponse from proto
	res := types.DescribeResponseFromProto(describeResponse)

	// add non-proto fields
	res.PluginName = pluginDef.Plugin

	return res, nil
}

func (p *PluginManager) Close() {
	p.pluginMutex.Lock()
	defer p.pluginMutex.Unlock()
	for _, plg := range p.Plugins {
		plg.Client.Kill()
	}
}

func (p *PluginManager) UpdateCollectionState(ctx context.Context, partition *config.Partition, fromTime time.Time, collectionStatePath string) error {
	// identify which plugin provides the source
	sourcePlugin, err := p.determineSourcePlugin(partition)
	if err != nil {
		return fmt.Errorf("error determining plugin for source %s: %w", partition.Source.Type, err)
	}

	// start plugin if needed
	pluginClient, err := p.getPlugin(sourcePlugin)
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

// formatToProto takes a config.Format, describes the format and returns the proto.FormatData for the plugin
func (p *PluginManager) formatToProto(ctx context.Context, format *config.Format, tablePlugin *pplugin.Plugin) (*proto.FormatData, error) {
	//  check if the format is provided by the table plugin or whether we need to start format plugin
	formatPluginName, ok := config.GetPluginForFormat(format)
	if !ok {
		return nil, fmt.Errorf("error determining plugin for format %s", format.FullName)
	}
	// if the format is provided by the table plugin, we can just convert it to proto
	if formatPluginName == tablePlugin.Plugin {
		slog.Info("format is provided by the table plugin - converting to proto", "format", format.FullName, "plugin", formatPluginName)
		return format.ToProto(), nil
	}

	// so the plugin is NOT the table plugin - start it if needed
	formatPlugin := pplugin.NewPlugin(formatPluginName)
	if formatPlugin.Plugin == tablePlugin.Plugin {
		// the format is provided by the table plugin - nothing to do
		return nil, nil
	}
	slog.Info("format is not provided by the table plugin - describing the format and converting to a regex format", "format", format.FullName, "plugin", formatPlugin.Plugin)

	// so the format is provided by a different plugin - start it if needed and execute a describe
	// if format is NOT a preset, we need to pass the custom formats to the describe call
	var opts []DescribeOpts
	if format.PresetName == "" {
		opts = append(opts, WithCustomFormats(format), WithCustomFormatsOnly())
	}

	describeResponse, err := p.Describe(ctx, formatPlugin.Plugin, opts...)
	if err != nil {
		return nil, fmt.Errorf("error resolving format: %w", err)
	}

	var desc *types.FormatDescription
	if format.PresetName != "" {
		// if format is a preset
		desc, ok = describeResponse.FormatPresets[format.PresetName]
		if !ok {
			return nil, fmt.Errorf("plugin '%s' returned no description for format preset '%s'", formatPluginName, format.PresetName)
		}
	} else {
		// we expect the custom format to be the one we asked for
		// the escriptions are keyed by unqualified name, i.e. type.name
		desc, ok = describeResponse.CustomFormats[format.UnqualifiedName]
		if !ok {
			return nil, fmt.Errorf("plugin '%s' returned no description for format '%s'", formatPluginName, format.UnqualifiedName)
		}
	}

	return &proto.FormatData{Name: format.FullName, Regex: desc.Regex}, nil
}

func (p *PluginManager) getSourcePluginReattach(ctx context.Context, partition *config.Partition, tablePlugin *pplugin.Plugin) (*grpc.PluginClient, *proto.SourcePluginReattach, error) {
	// identify which plugin provides the source
	sourcePlugin, err := p.determineSourcePlugin(partition)
	if err != nil {
		return nil, nil, fmt.Errorf("error determining plugin for source %s: %w", partition.Source.Type, err)
	}
	// if this plugin is different from the plugin that provides the table, we need to start the source plugin,
	// and then pass reattach info
	if sourcePlugin.Plugin == tablePlugin.Plugin {
		return nil, nil, nil
	}

	// so the source plugin is different from the table plugin - start if needed
	sourcePluginClient, err := p.getPlugin(sourcePlugin)
	if err != nil {
		return nil, nil, fmt.Errorf("error starting plugin '%s' required for source '%s': %w", sourcePlugin.Alias, partition.Source.Type, err)
	}
	sourcePluginReattach := proto.NewSourcePluginReattach(partition.Source.Type, sourcePlugin.Alias, sourcePluginClient.Client.ReattachConfig())

	return sourcePluginClient, sourcePluginReattach, nil
}

// getExecutionId generates a unique id based on the current time
// this can be passed into plugin calls to assist with tracking parallel calls
func getExecutionId() string {
	return fmt.Sprintf("%d%d", time.Now().Unix(), rand.Int32N(1000)) //nolint:gosec// strong enough for the execution id
}

// getPlugin returns the plugin client for the given plugin definition, starting the plugin if needed or returning
// the existing client if we have one
func (p *PluginManager) getPlugin(pluginDef *pplugin.Plugin) (*grpc.PluginClient, error) {
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

// startPlugin starts the plugin and returns the client
func (p *PluginManager) startPlugin(tp *pplugin.Plugin) (*grpc.PluginClient, error) {
	pluginName := tp.Alias

	// create the plugin map
	pluginMap := map[string]goplugin.Plugin{
		pluginName: &shared.TailpipeGRPCPlugin{},
	}

	cmd, err := p.getPluginCommand(tp)
	if err != nil {
		return nil, err
	}

	pluginStartTimeout := p.getPluginStartTimeout()
	c := goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  shared.Handshake,
		Plugins:          pluginMap,
		Cmd:              cmd,
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

func (p *PluginManager) getPluginCommand(tp *pplugin.Plugin) (*exec.Cmd, error) {
	pluginPath, err := pfilepaths.GetPluginPath(tp.Plugin, tp.Alias)
	if err != nil {
		return nil, fmt.Errorf("error getting plugin path for plugin '%s': %w", tp.Alias, err)
	}

	cmd := exec.Command("sh", "-c", pluginPath)

	// set the max memory for the plugin (if specified)
	maxMemoryBytes := tp.GetMaxMemoryBytes()
	if maxMemoryBytes != 0 {
		slog.Info("Setting max memory for plugin", "plugin", tp.Alias, "max memory (mb)", maxMemoryBytes/(1024*1024))
		// set GOMEMLIMIT for the plugin command env
		cmd.Env = append(os.Environ(), fmt.Sprintf("GOMEMLIMIT=%d", maxMemoryBytes))
	}
	return cmd, nil
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

func (p *PluginManager) readCollectionEvents(ctx context.Context, executionId string, pluginStream proto.TailpipePlugin_AddObserverClient) {
	pluginEventChan := make(chan events.Event)

	slog.Info("starting to read plugin events")
	// goroutine to read the plugin event stream and send the events down the event channel
	go func() {
		defer func() {
			if r := recover(); r != nil {
				pluginEventChan <- events.NewCompletedEvent(executionId, 0, 0, gokithelpers.ToError(r))
			}
			// ensure
			close(pluginEventChan)
		}()

		for {
			pe, err := pluginStream.Recv()
			if err != nil {
				slog.Error("error reading from plugin stream", "error", err)
				// clean up the error
				err = cleanupPluginError(err)
				// send a completion event with an error
				pluginEventChan <- events.NewCompletedEvent(executionId, 0, 0, err)
				return
			}
			e, err := events.EventFromProto(pe)
			if err != nil {
				slog.Info("error converting plugin event to proto", "error", err)
				// send a completion event with an error
				pluginEventChan <- events.NewCompletedEvent(executionId, 0, 0, err)
				return
			}

			pluginEventChan <- e
			// if this is a completion event , stop polling
			if pe.GetCompleteEvent() != nil {
				slog.Info("got completion event - stop polling and close plugin event channel")
				return
			}
		}
	}()

	// loop until either:
	// - the context is cancelled
	// - there is an error reading from the plugin stream
	// - the pluginEventChan is closed (following a completion event)
	for {
		select {
		case <-ctx.Done():
			return

		case ev := <-pluginEventChan:
			// convert the protobuf event to an observer event
			// and send it to the observer
			if ev == nil {
				// channel is closed
				return
			}
			err := p.obs.Notify(ctx, ev)
			if err != nil {
				// if notify fails, send a completion event with the error
				if err = p.obs.Notify(ctx, events.NewCompletedEvent(executionId, 0, 0, err)); err != nil {
					slog.Error("error notifying observer of error", "error", err)
				}
				return
			}
		}
	}

}

// determineSourcePlugin determines plugin which provides the given source type for the given partition
// try to use the source information registered in the version file
// if older plugins are installed which did not register the source type, then fall back to deducing the plugin name
func (p *PluginManager) determineSourcePlugin(partition *config.Partition) (*pplugin.Plugin, error) {
	sourceType := partition.Source.Type
	// because we reference the core plugin, all sources it provides are registered with our source factory instance
	coreSources, err := core.DescribeSources()
	if err != nil {
		return nil, fmt.Errorf("error describing sources: %w", err)
	}
	if _, ok := coreSources[sourceType]; ok {
		// Rather than hard code to core@latest, call CorePluginInstallStream
		// to handle the case where the core plugin is not installed
		coreName := constants.CorePluginInstallStream()
		return pplugin.NewPlugin(coreName), nil
	}

	pluginName := config.GetPluginForSourceType(sourceType, config.GlobalConfig.PluginVersions)

	// now return the plugin
	return pplugin.NewPlugin(pluginName), nil
}

// EnsureCorePlugin ensures the core plugin is installed or the min version is satisfied
func EnsureCorePlugin(ctx context.Context) (*versionfile.PluginVersionFile, error) {
	// load the plugin version file
	pluginVersions, err := loadPluginVersionFile(ctx)
	if err != nil {
		return nil, err
	}

	// get the installation state
	state, err := installationstate.Load()
	if err != nil {
		return nil, err
	}

	action := "Installing"

	// check if core plugin is already installed
	corePluginRequiredConstraint := constants.CorePluginRequiredVersionConstraint()
	corePluginStream := constants.CorePluginInstallStream()

	exists, _ := pplugin.Exists(ctx, corePluginStream)
	if exists {
		// check if the min version is satisfied; if not then update

		// find the version of the core plugin from the pluginVersions
		// NOTE: use the prefixed name to index the pluginVersions map
		fullName := constants.CorePluginFullName()
		installedVersion := pluginVersions.Plugins[fullName].Version

		// if installed version is 'local', that will do
		if installedVersion == "local" {
			return pluginVersions, nil
		}

		// compare the version(using semver) with the min version
		satisfy, err := versionSatisfyVersionConstraint(installedVersion, corePluginRequiredConstraint)
		if err != nil {
			return nil, err
		}
		// if satisfied - we are done
		if satisfy {
			return pluginVersions, nil
		}

		// so an update is required - set action to updating and fall through to installation
		action = "Updating"
	}
	// install the core plugin
	if err = installCorePlugin(ctx, state, action, corePluginStream); err != nil {
		return nil, err
	}

	// now reload the version file as there may be new metadata for the cor plugin
	return loadPluginVersionFile(ctx)
}

func loadPluginVersionFile(ctx context.Context) (*versionfile.PluginVersionFile, error) {
	// load plugin versions
	// NOTE we must do this before LoadTailpipeConfig as we need them for EnsureCorePlugin
	pluginVersions, err := versionfile.LoadPluginVersionFile(ctx)
	if err != nil {
		return nil, err
	}

	// TODO KAI CHECK THIS
	// add any "local" plugins (i.e. plugins installed under the 'local' folder) into the version file
	ew := pluginVersions.AddLocalPlugins(ctx)
	if ew.Error != nil {
		return nil, ew.Error
	}
	return pluginVersions, nil
}

func installCorePlugin(ctx context.Context, state installationstate.InstallationState, operation string, pluginStream string) error {
	spinner := statushooks.NewStatusSpinnerHook()
	spinner.Show()
	defer spinner.Hide()
	spinner.SetStatus(fmt.Sprintf("%s core plugin", operation))

	// get a ref for the plugin stream
	ref := pociinstaller.NewImageRef(pluginStream)
	org, name, constraint := ref.GetOrgNameAndStream()

	rpv, err := pplugin.GetLatestPluginVersionByConstraint(ctx, state.InstallationID, org, name, constraint)
	if err != nil {
		return err
	}
	resolvedPlugin := *rpv

	progress := make(chan struct{}, 5)

	// install plugin
	_, err = Install(ctx, resolvedPlugin, progress, constants.BaseImageRef, ociinstaller.TailpipeMediaTypeProvider{})
	if err != nil {
		return err
	}
	return nil
}

func versionSatisfyVersionConstraint(ver string, pluginVersion string) (bool, error) {
	// check if the version satisfies the min version requirement of core plugin
	// Parse the versions
	installedVer, err := version.NewVersion(ver)
	if err != nil {
		return false, err
	}
	versionConstraint, err := version.NewConstraint(pluginVersion)
	if err != nil {
		return false, err
	}

	return versionConstraint.Check(installedVer), nil
}

func IsNotImplementedError(err error) bool {
	status, ok := status.FromError(err)
	if !ok {
		return false
	}

	return status.Code() == codes.Unimplemented
}
