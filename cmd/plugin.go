package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/gosuri/uiprogress"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/installationstate"
	pociinstaller "github.com/turbot/pipe-fittings/ociinstaller"
	pplugin "github.com/turbot/pipe-fittings/plugin"
	"github.com/turbot/pipe-fittings/querydisplay"
	"github.com/turbot/pipe-fittings/statushooks"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/pipe-fittings/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/ociinstaller"
	"github.com/turbot/tailpipe/internal/plugin"
	"github.com/turbot/tailpipe/internal/plugin_manager"
)

type installedPlugin struct {
	Name       string   `json:"name"`
	Version    string   `json:"version"`
	Partitions []string `json:"partitions"`
}

type failedPlugin struct {
	Name        string   `json:"name"`
	Reason      string   `json:"reason"`
	Connections []string `json:"connections"`
}

type pluginJsonOutput struct {
	Installed []installedPlugin `json:"installed"`
	Failed    []failedPlugin    `json:"failed"`
	Warnings  []string          `json:"warnings"`
}

// Plugin management commands
func pluginCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "plugin [command]",
		Args:  cobra.NoArgs,
		Short: "Steampipe plugin management",
		Long: `Steampipe plugin management.

Plugins extend Steampipe to work with many different services and providers.
Find plugins using the public registry at https://hub.tailpipe.io.

Examples:

  # Install a plugin
  tailpipe plugin install aws

  # Update a plugin
  tailpipe plugin update aws

  # List installed plugins
  tailpipe plugin list

  # Uninstall a plugin
  tailpipe plugin uninstall aws`,
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			utils.LogTime("cmd.plugin.PersistentPostRun start")
			defer utils.LogTime("cmd.plugin.PersistentPostRun end")
			pplugin.CleanupOldTmpDirs(cmd.Context())
		},
	}
	cmd.AddCommand(pluginInstallCmd())
	cmd.AddCommand(pluginListCmd())
	cmd.AddCommand(pluginShowCmd())
	cmd.AddCommand(pluginUninstallCmd())
	cmd.AddCommand(pluginUpdateCmd())
	cmd.Flags().BoolP(pconstants.ArgHelp, "h", false, "Help for plugin")

	return cmd
}

// Install a plugin
func pluginInstallCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "install [flags] [registry/org/]name[@version]",
		Args:  cobra.ArbitraryArgs,
		Run:   runPluginInstallCmd,
		Short: "Install one or more plugins",
		Long: `Install one or more plugins.

Install a Steampipe plugin, making it available for queries and configuration.
The plugin name format is [registry/org/]name[@version]. The default
registry is hub.tailpipe.io, default org is turbot and default version
is latest. The name is a required argument.

Examples:

  # Install all missing plugins that are specified in configuration files
  tailpipe plugin install

  # Install a common plugin (turbot/aws)
  tailpipe plugin install aws

  # Install a specific plugin version
  tailpipe plugin install turbot/azure@0.1.0

  # Hide progress bars during installation
  tailpipe plugin install --progress=false aws

  # Skip creation of default plugin config file
  tailpipe plugin install --skip-config aws`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddBoolFlag(pconstants.ArgProgress, true, "Display installation progress").
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin install", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

// Update plugins
func pluginUpdateCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "update [flags] [registry/org/]name[@version]",
		Args:  cobra.ArbitraryArgs,
		Run:   runPluginUpdateCmd,
		Short: "Update one or more plugins",
		Long: `Update plugins.

Update one or more Steampipe plugins, making it available for queries and configuration.
The plugin name format is [registry/org/]name[@version]. The default
registry is hub.tailpipe.io, default org is turbot and default version
is latest. The name is a required argument.

Examples:

  # Update all plugins to their latest available version
  tailpipe plugin update --all

  # Update a common plugin (turbot/aws)
  tailpipe plugin update aws

  # Hide progress bars during update
  tailpipe plugin update --progress=false aws`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddBoolFlag(pconstants.ArgAll, false, "Update all plugins to its latest available version").
		AddBoolFlag(pconstants.ArgProgress, true, "Display installation progress").
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin update", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

// variable used to assign the output mode flag
var pluginOutputMode = constants.PluginOutputModeTable

// List plugins
func pluginListCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Run:   runPluginListCmd,
		Short: "List currently installed plugins",
		Long: `List currently installed plugins.

List all Steampipe plugins installed for this user.

Examples:

  # List installed plugins
  tailpipe plugin list

  # List plugins that have updates available
  tailpipe plugin list --outdated

  # List plugins output in json
  tailpipe plugin list --output json`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin list", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

// Show plugin
func pluginShowCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:  "show <plugin>",
		Args: cobra.ExactArgs(1),
		Run:  runPluginShowCmd,
		// TODO improve descriptions
		Short: "Show details of a plugin",
		Long:  `Show the tables and sources provided by plugin`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddVarFlag(enumflag.New(&pluginOutputMode, pconstants.ArgOutput, constants.PluginOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", "))).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin show", cmdconfig.FlagOptions.WithShortHand("h"))
	return cmd
}

// Uninstall a plugin
func pluginUninstallCmd() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "uninstall [flags] [registry/org/]name",
		Args:  cobra.ArbitraryArgs,
		Run:   runPluginUninstallCmd,
		Short: "Uninstall a plugin",
		Long: `Uninstall a plugin.

Uninstall a Steampipe plugin, removing it from use. The plugin name format is
[registry/org/]name. (Version is not relevant in uninstall, since only one
version of a plugin can be installed at a time.)

Example:

  # Uninstall a common plugin (turbot/aws)
  tailpipe plugin uninstall aws

`,
	}

	cmdconfig.OnCmd(cmd).
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin uninstall", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

var pluginInstallSteps = []string{
	"Downloading",
	"Installing Plugin",
	"Installing Docs",
	"Installing Config",
	"Updating Tailpipe",
	"Done",
}

func runPluginInstallCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	utils.LogTime("runPluginInstallCmd install")
	defer func() {
		utils.LogTime("runPluginInstallCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	// args to 'plugin install' -- one or more plugins to install
	// plugin names can be simple names for "standard" plugins, constraint suffixed names
	// or full refs to the OCI image
	// - aws
	// - aws@0.118.0
	// - aws@^0.118
	// - ghcr.io/turbot/tailpipe/plugins/turbot/aws:1.0.0
	plugins := append([]string{}, args...)
	showProgress := viper.GetBool(pconstants.ArgProgress)
	installReports := make(pplugin.PluginInstallReports, 0, len(plugins))

	if len(plugins) == 0 {
		if len(config.GlobalConfig.Plugins) == 0 {
			error_helpers.ShowError(ctx, errors.New("no plugins installed"))
			exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
			return
		}

		// get the list of plugins to install
		for imageRef := range config.GlobalConfig.Plugins {
			ref := pociinstaller.NewImageRef(imageRef)
			plugins = append(plugins, ref.GetFriendlyName())
		}
	}

	state, err := installationstate.Load()
	if err != nil {
		error_helpers.ShowError(ctx, fmt.Errorf("could not load state"))
		exitCode = pconstants.ExitCodePluginLoadingError
		return
	}

	// a leading blank line - since we always output multiple lines
	fmt.Println() //nolint:forbidigo // ui output
	progressBars := uiprogress.New()
	installWaitGroup := &sync.WaitGroup{}
	reportChannel := make(chan *pplugin.PluginInstallReport, len(plugins))

	if showProgress {
		progressBars.Start()
	}
	for _, pluginName := range plugins {
		installWaitGroup.Add(1)
		bar := createProgressBar(pluginName, progressBars)

		ref := pociinstaller.NewImageRef(pluginName)
		org, name, constraint := ref.GetOrgNameAndStream()
		orgAndName := fmt.Sprintf("%s/%s", org, name)
		var resolved pplugin.ResolvedPluginVersion
		if ref.IsFromTurbotHub() {
			// TODO: #plugin replace GetLatestPluginVersionByConstraint with method in pipe-fittings once hub api is ready
			rpv, err := GetLatestPluginVersionByConstraint(ctx, state.InstallationID, org, name, constraint)
			if err != nil || rpv == nil {
				report := &pplugin.PluginInstallReport{
					Plugin:         pluginName,
					Skipped:        true,
					SkipReason:     pconstants.InstallMessagePluginNotFound,
					IsUpdateReport: false,
				}
				reportChannel <- report
				installWaitGroup.Done()
				continue
			}
			resolved = *rpv
		} else {
			resolved = pplugin.NewResolvedPluginVersion(orgAndName, constraint, constraint)
		}

		go doPluginInstall(ctx, bar, pluginName, resolved, installWaitGroup, reportChannel)
	}
	go func() {
		installWaitGroup.Wait()
		close(reportChannel)
	}()
	installCount := 0
	for report := range reportChannel {
		installReports = append(installReports, report)
		if !report.Skipped {
			installCount++
		} else if !(report.Skipped && report.SkipReason == "Already installed") {
			exitCode = pconstants.ExitCodePluginInstallFailure
		}
	}
	if showProgress {
		progressBars.Stop()
	}

	pplugin.PrintInstallReports(installReports, false)

	// a concluding blank line - since we always output multiple lines
	fmt.Println() //nolint:forbidigo // ui output
}

func doPluginInstall(ctx context.Context, bar *uiprogress.Bar, pluginName string, resolvedPlugin pplugin.ResolvedPluginVersion, wg *sync.WaitGroup, returnChannel chan *pplugin.PluginInstallReport) {
	var report *pplugin.PluginInstallReport

	pluginAlreadyInstalled, _ := pplugin.Exists(ctx, pluginName)
	if pluginAlreadyInstalled {
		// set the bar to MAX
		//nolint:golint,errcheck // the error happens if we set this over the max value
		bar.Set(len(pluginInstallSteps))
		// let the bar append itself with "Already Installed"
		bar.AppendFunc(func(b *uiprogress.Bar) string {
			return helpers.Resize(pconstants.InstallMessagePluginAlreadyInstalled, 20)
		})
		report = &pplugin.PluginInstallReport{
			Plugin:         pluginName,
			Skipped:        true,
			SkipReason:     pconstants.InstallMessagePluginAlreadyInstalled,
			IsUpdateReport: false,
		}
	} else {
		// let the bar append itself with the current installation step
		bar.AppendFunc(func(b *uiprogress.Bar) string {
			if report != nil && report.SkipReason == pconstants.InstallMessagePluginNotFound {
				return helpers.Resize(pconstants.InstallMessagePluginNotFound, 20)
			} else {
				if b.Current() == 0 {
					// no install step to display yet
					return ""
				}
				return helpers.Resize(pluginInstallSteps[b.Current()-1], 20)
			}
		})

		report = installPlugin(ctx, resolvedPlugin, false, bar)
	}
	returnChannel <- report
	wg.Done()
}

func runPluginUpdateCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	utils.LogTime("runPluginUpdateCmd start")
	defer func() {
		utils.LogTime("runPluginUpdateCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	// args to 'plugin update' -- one or more plugins to update
	// These can be simple names for "standard" plugins, constraint suffixed names
	// or full refs to the OCI image
	// - aws
	// - aws@0.118.0
	// - aws@^0.118
	// - ghcr.io/turbot/tailpipe/plugins/turbot/aws:1.0.0
	plugins, err := resolveUpdatePluginsFromArgs(args)
	showProgress := viper.GetBool(pconstants.ArgProgress)

	if err != nil {
		fmt.Println()
		error_helpers.ShowError(ctx, err)
		fmt.Println()
		cmd.Help() //nolint:errcheck // we are not interested in the error
		fmt.Println()
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		return
	}

	if len(plugins) > 0 && !(cmdconfig.Viper().GetBool(pconstants.ArgAll)) && plugins[0] == pconstants.ArgAll {
		// improve the response to wrong argument "tailpipe plugin update all"
		fmt.Println()
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		error_helpers.ShowError(ctx, fmt.Errorf("Did you mean %s?", pconstants.Bold("--all")))
		fmt.Println()
		return
	}

	state, err := installationstate.Load()
	if err != nil {
		error_helpers.ShowError(ctx, fmt.Errorf("could not load state"))
		exitCode = pconstants.ExitCodePluginLoadingError
		return
	}

	// retrieve the plugin version data from tailpipe config
	pluginVersions := config.GlobalConfig.PluginVersions

	var runUpdatesFor []*versionfile.InstalledVersion
	updateResults := make(pplugin.PluginInstallReports, 0, len(plugins))

	// a leading blank line - since we always output multiple lines
	fmt.Println()

	if cmdconfig.Viper().GetBool(pconstants.ArgAll) {
		for k, v := range pluginVersions {
			ref := pociinstaller.NewImageRef(k)
			org, name, constraint := ref.GetOrgNameAndStream()
			key := fmt.Sprintf("%s/%s@%s", org, name, constraint)

			plugins = append(plugins, key)
			runUpdatesFor = append(runUpdatesFor, v)
		}
	} else {
		// get the args and retrieve the installed versions
		for _, p := range plugins {
			ref := pociinstaller.NewImageRef(p)
			isExists, _ := pplugin.Exists(ctx, p)
			if isExists {
				if strings.HasPrefix(ref.DisplayImageRef(), constants.TailpipeHubOCIBase) {
					runUpdatesFor = append(runUpdatesFor, pluginVersions[ref.DisplayImageRef()])
				} else {
					error_helpers.ShowError(ctx, fmt.Errorf("cannot check updates for plugins not distributed via hub.tailpipe.io, you should uninstall then reinstall the plugin to get the latest version"))
					exitCode = pconstants.ExitCodePluginLoadingError
					return
				}
			} else {
				exitCode = pconstants.ExitCodePluginNotFound
				updateResults = append(updateResults, &pplugin.PluginInstallReport{
					Skipped:        true,
					Plugin:         p,
					SkipReason:     pconstants.InstallMessagePluginNotInstalled,
					IsUpdateReport: true,
				})
			}
		}
	}

	if len(plugins) == len(updateResults) {
		// we have report for all
		// this may happen if all given plugins are
		// not installed
		pplugin.PrintInstallReports(updateResults, true)
		fmt.Println()
		return
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	statushooks.SetStatus(ctx, "Checking for available updates")
	// TODO: #plugin replace GetUpdateReport with method in pipe-fittings once hub api is ready
	reports := GetUpdateReport(timeoutCtx, state.InstallationID, runUpdatesFor)
	statushooks.Done(ctx)
	if len(reports) == 0 {
		// this happens if for some reason the update server could not be contacted,
		// in which case we get back an empty map
		error_helpers.ShowError(ctx, fmt.Errorf("there was an issue contacting the update server, please try later"))
		exitCode = pconstants.ExitCodePluginLoadingError
		return
	}

	updateWaitGroup := &sync.WaitGroup{}
	reportChannel := make(chan *pplugin.PluginInstallReport, len(reports))
	progressBars := uiprogress.New()
	if showProgress {
		progressBars.Start()
	}

	sorted := utils.SortedMapKeys(reports)
	for _, key := range sorted {
		report := reports[key]
		updateWaitGroup.Add(1)
		bar := createProgressBar(report.ShortNameWithConstraint(), progressBars)
		go doPluginUpdate(ctx, bar, report, updateWaitGroup, reportChannel)
	}
	go func() {
		updateWaitGroup.Wait()
		close(reportChannel)
	}()
	installCount := 0

	for updateResult := range reportChannel {
		updateResults = append(updateResults, updateResult)
		if !updateResult.Skipped {
			installCount++
		}
	}
	if showProgress {
		progressBars.Stop()
	}

	pplugin.PrintInstallReports(updateResults, true)

	// a concluding blank line - since we always output multiple lines
	fmt.Println()
}

func doPluginUpdate(ctx context.Context, bar *uiprogress.Bar, pvr pplugin.PluginVersionCheckReport, wg *sync.WaitGroup, returnChannel chan *pplugin.PluginInstallReport) {
	var report *pplugin.PluginInstallReport

	if pplugin.UpdateRequired(pvr) {
		// update required, resolve version and install update
		bar.AppendFunc(func(b *uiprogress.Bar) string {
			// set the progress bar to append itself  with the step underway
			if b.Current() == 0 {
				// no install step to display yet
				return ""
			}
			return helpers.Resize(pluginInstallSteps[b.Current()-1], 20)
		})
		rp := pplugin.NewResolvedPluginVersion(pvr.ShortName(), pvr.CheckResponse.Version, pvr.CheckResponse.Constraint)
		report = installPlugin(ctx, rp, true, bar)
	} else {
		// update NOT required, return already installed report
		bar.AppendFunc(func(b *uiprogress.Bar) string {
			// set the progress bar to append itself with "Already Installed"
			return helpers.Resize(pconstants.InstallMessagePluginLatestAlreadyInstalled, 30)
		})
		// set the progress bar to the maximum
		bar.Set(len(pluginInstallSteps)) //nolint:golint,errcheck // the error happens if we set this over the max value
		report = &pplugin.PluginInstallReport{
			Plugin:         fmt.Sprintf("%s@%s", pvr.CheckResponse.Name, pvr.CheckResponse.Constraint),
			Skipped:        true,
			SkipReason:     pconstants.InstallMessagePluginLatestAlreadyInstalled,
			IsUpdateReport: true,
		}
	}

	returnChannel <- report
	wg.Done()
}

func createProgressBar(plugin string, parentProgressBars *uiprogress.Progress) *uiprogress.Bar {
	bar := parentProgressBars.AddBar(len(pluginInstallSteps))
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return helpers.Resize(plugin, 30)
	})
	return bar
}

func installPlugin(ctx context.Context, resolvedPlugin pplugin.ResolvedPluginVersion, isUpdate bool, bar *uiprogress.Bar) *pplugin.PluginInstallReport {
	// start a channel for progress publications from plugin.Install
	progress := make(chan struct{}, 5)
	defer func() {
		// close the progress channel
		close(progress)
	}()
	go func() {
		for {
			// wait for a message on the progress channel
			<-progress
			// increment the progress bar
			bar.Incr()
		}
	}()

	image, err := plugin.Install(ctx, resolvedPlugin, progress, constants.TailpipeHubOCIBase, ociinstaller.TailpipeMediaTypeProvider{}, pociinstaller.WithSkipConfig(viper.GetBool(pconstants.ArgSkipConfig)))
	if err != nil {
		msg := ""
		// used to build data for the plugin install report to be used for display purposes
		_, name, constraint := pociinstaller.NewImageRef(resolvedPlugin.GetVersionTag()).GetOrgNameAndStream()
		if isPluginNotFoundErr(err) {
			exitCode = pconstants.ExitCodePluginNotFound
			msg = pconstants.InstallMessagePluginNotFound
		} else {
			msg = err.Error()
		}
		return &pplugin.PluginInstallReport{
			Plugin:         fmt.Sprintf("%s@%s", name, constraint),
			Skipped:        true,
			SkipReason:     msg,
			IsUpdateReport: isUpdate,
		}
	}

	// used to build data for the plugin install report to be used for display purposes
	org, name, _ := image.ImageRef.GetOrgNameAndStream()
	versionString := ""
	if image.Config.Plugin.Version != "" {
		versionString = " v" + image.Config.Plugin.Version
	}
	docURL := fmt.Sprintf("https://hub.tailpipe.io/plugins/%s/%s", org, name)
	if !image.ImageRef.IsFromTurbotHub() {
		docURL = fmt.Sprintf("https://%s/%s", org, name)
	}
	return &pplugin.PluginInstallReport{
		Plugin:         fmt.Sprintf("%s@%s", name, resolvedPlugin.Constraint),
		Skipped:        false,
		Version:        versionString,
		DocURL:         docURL,
		IsUpdateReport: isUpdate,
	}
}

func isPluginNotFoundErr(err error) bool {
	return strings.HasSuffix(err.Error(), "not found")
}

func resolveUpdatePluginsFromArgs(args []string) ([]string, error) {
	plugins := append([]string{}, args...)

	if len(plugins) == 0 && !(cmdconfig.Viper().GetBool("all")) {
		// either plugin name(s) or "all" must be provided
		return nil, fmt.Errorf("you need to provide at least one plugin to update or use the %s flag", pconstants.Bold("--all"))
	}

	if len(plugins) > 0 && cmdconfig.Viper().GetBool(pconstants.ArgAll) {
		// we can't allow update and install at the same time
		return nil, fmt.Errorf("%s cannot be used when updating specific plugins", pconstants.Bold("`--all`"))
	}

	return plugins, nil
}

func runPluginListCmd(cmd *cobra.Command, _ []string) {
	//setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)
	outputFormat := viper.GetString(pconstants.ArgOutput)

	utils.LogTime("runPluginListCmd list")
	defer func() {
		utils.LogTime("runPluginListCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	pluginList, err := plugin.List(ctx, config.GlobalConfig.PluginVersions)
	error_helpers.FailOnError(err)

	err = showPluginListOutput(pluginList, outputFormat)
	if err != nil {
		error_helpers.ShowError(ctx, err)
	}

}

func showPluginListOutput(pluginList []plugin.PluginListItem, outputFormat string) error {
	switch outputFormat {
	case pconstants.OutputFormatTable:
		showPluginListAsTable(pluginList)
		return nil
	case pconstants.OutputFormatJSON:
		return showPluginListAsJSON(pluginList)
	default:
		return errors.New("invalid output format")
	}
}

func showPluginListAsTable(pluginList []plugin.PluginListItem) {
	headers := []string{"Installed", "Version", "Partitions"}
	var rows [][]string
	// List installed plugins in a table
	if len(pluginList) != 0 {
		for _, item := range pluginList {
			rows = append(rows, []string{item.Name, item.Version.String(), strings.Join(item.Partitions, ",")})
		}
	} else {
		rows = append(rows, []string{"", "", ""})
	}
	querydisplay.ShowWrappedTable(headers, rows, &querydisplay.ShowWrappedTableOptions{AutoMerge: false})
	fmt.Printf("\n")
}

func showPluginListAsJSON(pluginList []plugin.PluginListItem) error {
	output := pluginJsonOutput{}

	for _, item := range pluginList {
		installed := installedPlugin{
			Name:       item.Name,
			Version:    item.Version.String(),
			Partitions: item.Partitions,
		}
		output.Installed = append(output.Installed, installed)
	}

	jsonOutput, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(jsonOutput))
	fmt.Println()
	return nil
}

func runPluginShowCmd(cmd *cobra.Command, args []string) {
	// we expect 1 argument, the plugin name
	if len(args) != 1 {
		error_helpers.ShowError(cmd.Context(), fmt.Errorf("you need to provide the name of a plugin"))
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		return
	}

	// now ask the plugin manager to make a describe call
	pluginManager := plugin_manager.New()
	describeResponse, err := pluginManager.Describe(cmd.Context(), args[0])
	error_helpers.FailOnError(err)

	err = showPluginShowOutput(describeResponse, viper.GetString(pconstants.ArgOutput))
	if err != nil {
		error_helpers.ShowError(cmd.Context(), err)
		exitCode = pconstants.ExitCodePluginListFailure
	}
}

func showPluginShowOutput(resp *plugin_manager.DescribeResponse, outputFormat string) error {
	switch outputFormat {
	case pconstants.OutputFormatTable:
		showPluginShowAsTable(resp)
		return nil
	case pconstants.OutputFormatJSON:
		return showPluginShowAsJSON(resp)
	default:
		return errors.New("invalid output format")
	}
}

func showPluginShowAsTable(resp *plugin_manager.DescribeResponse) {
	fmt.Println("table output not implemented")
	//headers := []string{"Installed", "Version", "Partitions"}
	//var rows [][]string
	//// Show installed plugins in a table
	//if len(pluginShow) != 0 {
	//	for _, item := range pluginShow {
	//		rows = append(rows, []string{item.Name, item.Version.String(), strings.Join(item.Partitions, ",")})
	//	}
	//} else {
	//	rows = append(rows, []string{"", "", ""})
	//}
	//querydisplay.ShowWrappedTable(headers, rows, &querydisplay.ShowWrappedTableOptions{AutoMerge: false})
	//fmt.Printf("\n")
}

func showPluginShowAsJSON(pluginShow *plugin_manager.DescribeResponse) error {

	jsonOutput, err := json.MarshalIndent(pluginShow, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(jsonOutput))
	fmt.Println()
	return nil
}

func runPluginUninstallCmd(cmd *cobra.Command, args []string) {
	// setup a cancel context and start cancel handler
	ctx, cancel := context.WithCancel(cmd.Context())
	contexthelpers.StartCancelHandler(cancel)

	utils.LogTime("runPluginUninstallCmd uninstall")

	defer func() {
		utils.LogTime("runPluginUninstallCmd end")
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
			exitCode = pconstants.ExitCodeUnknownErrorPanic
		}
	}()

	if len(args) == 0 {
		fmt.Println()
		error_helpers.ShowError(ctx, fmt.Errorf("you need to provide at least one plugin to uninstall"))
		fmt.Println()
		cmd.Help() //nolint:errcheck // we are not interested in the error
		fmt.Println()
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		return
	}

	reports := plugin.PluginRemoveReports{}
	statushooks.SetStatus(ctx, fmt.Sprintf("Uninstalling %s", utils.Pluralize("plugin", len(args))))
	for _, p := range args {
		statushooks.SetStatus(ctx, fmt.Sprintf("Uninstalling %s", p))
		if report, err := plugin.Remove(ctx, p); err != nil {
			if strings.Contains(err.Error(), "not found") {
				exitCode = pconstants.ExitCodePluginNotFound
			}
			error_helpers.ShowErrorWithMessage(ctx, err, fmt.Sprintf("Failed to uninstall plugin '%s'", p))
		} else {
			report.ShortName = p
			reports = append(reports, *report)
		}
	}
	statushooks.Done(ctx)
	reports.Print()
}

//

// GetLatestPluginVersionByConstraint // TODO: #plugin remove this and use variant in pipe-fittings once hub API is available
func GetLatestPluginVersionByConstraint(ctx context.Context, installationID, org, name, constraint string) (*pplugin.ResolvedPluginVersion, error) {
	orgAndName := fmt.Sprintf("%s/%s", org, name)

	version, err := getLatestVersionFromGHCR(ctx, org, name, constraint, "0.0.0")
	if err != nil {
		return nil, err
	}

	rpv := pplugin.NewResolvedPluginVersion(orgAndName, version, constraint)
	return &rpv, nil
}

// getLatestVersionsForPlugins // TODO: #plugin remove this and use variant in pipe-fittings once hub API is available
func getLatestVersionsForPlugins(ctx context.Context, plugins []*versionfile.InstalledVersion) map[string]pplugin.PluginVersionCheckReport {
	reports := make(map[string]pplugin.PluginVersionCheckReport)

	for _, p := range plugins {
		ref := pociinstaller.NewImageRef(p.Name)
		org, name, constraint := ref.GetOrgNameAndStream()
		mapKey := fmt.Sprintf("%s/%s/%s", org, name, constraint)

		version, _ := getLatestVersionFromGHCR(ctx, org, name, constraint, p.Version)
		r := pplugin.PluginVersionCheckReport{Plugin: p}
		r.CheckResponse.Name = name
		r.CheckResponse.Org = org
		r.CheckResponse.Version = version
		r.CheckResponse.Constraint = constraint

		reports[mapKey] = r
	}

	return reports
}

// GetUpdateReport // TODO: #plugin remove this and use variant in pipe-fittings once hub API is available
func GetUpdateReport(ctx context.Context, installationID string, plugins []*versionfile.InstalledVersion) map[string]pplugin.PluginVersionCheckReport {
	if len(plugins) == 0 {
		return nil
	}

	versionFile, err := versionfile.LoadPluginVersionFile(ctx)
	if err != nil {
		return nil
	}

	reports := getLatestVersionsForPlugins(ctx, plugins)

	for k, v := range reports {
		if v.CheckResponse.Name == "" {
			delete(reports, k)
		}
	}

	for _, p := range plugins {
		versionFile.Plugins[p.Name].LastCheckedDate = utils.FormatTime(time.Now())
	}

	if err = versionFile.Save(); err != nil {
		return nil
	}

	return reports
}

// getLatestVersionFromGHCR // TODO: #plugin remove this and use variant in pipe-fittings once hub API is available
func getLatestVersionFromGHCR(ctx context.Context, org, name, constraint, currentVersion string) (string, error) {
	baseUrl := "https://ghcr.io"
	url := fmt.Sprintf("%s/v2/turbot/tailpipe/plugins/%s/%s/tags/list", baseUrl, org, name)
	latestSemver, _ := semver.NewVersion(currentVersion)
	if constraint == "latest" {
		constraint = "*"
	}
	constraintSemver, _ := semver.NewConstraint(constraint)

	client := &http.Client{}
	token := os.Getenv("GHCR_WEB_TOKEN") // NOTE: #tactical we **NEED** this token to be set in the environment for now, will use token in API when moved to hub API
	if token == "" {
		return "", fmt.Errorf("GHCR_WEB_TOKEN environment variable is not set, this is needed for now. Use GITHUB_TOKEN w/package:read permissions base64 encoded")
	}

	for {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", err
		}

		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to make request: %v", err)
		}

		if resp.StatusCode == 404 {
			return "", fmt.Errorf("plugin tags for org:[%s] name:[%s] not found", org, name)
		} else if resp.StatusCode != 200 {
			return "", fmt.Errorf("unexpected error: %s", resp.Status)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read response body: %v", err)
		}

		var tagsResp tagsResponse
		if err := json.Unmarshal(body, &tagsResp); err != nil {
			return "", fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		// close here, defer will be too late - possible resource leak
		_ = resp.Body.Close()

		for _, tag := range tagsResp.Tags {
			t, err := semver.NewVersion(tag)
			if err != nil {
				continue // tag wasn't a semver ignore it
			}

			if constraintSemver.Check(t) && t.GreaterThan(latestSemver) {
				latestSemver = t
			}
		}

		linkHeader := resp.Header.Get("Link")
		if linkHeader != "" {
			re := regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)
			matches := re.FindStringSubmatch(linkHeader)
			if len(matches) > 1 {
				url = fmt.Sprintf("%s%s", baseUrl, matches[1])
			}
		} else {
			break
		}

	}

	return latestSemver.String(), nil
}

// tagsResponse // TODO: #plugin remove this and use variant in pipe-fittings once hub API is available
type tagsResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}
