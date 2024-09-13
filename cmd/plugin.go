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
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/contexthelpers"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/installationstate"
	pociinstaller "github.com/turbot/pipe-fittings/ociinstaller"
	pplugin "github.com/turbot/pipe-fittings/plugin"
	"github.com/turbot/pipe-fittings/statushooks"
	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/pipe-fittings/versionfile"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/ociinstaller"
	"github.com/turbot/tailpipe/internal/plugin"
)

type installedPlugin struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Connections []string `json:"connections"`
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
Find plugins using the public registry at https://hub.steampipe.io.

Examples:

  # Install a plugin
  steampipe plugin install aws

  # Update a plugin
  steampipe plugin update aws

  # List installed plugins
  steampipe plugin list

  # Uninstall a plugin
  steampipe plugin uninstall aws`,
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			utils.LogTime("cmd.plugin.PersistentPostRun start")
			defer utils.LogTime("cmd.plugin.PersistentPostRun end")
			pplugin.CleanupOldTmpDirs(cmd.Context())
		},
	}
	cmd.AddCommand(pluginInstallCmd())
	cmd.AddCommand(pluginListCmd())
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
registry is hub.steampipe.io, default org is turbot and default version
is latest. The name is a required argument.

Examples:

  # Install all missing plugins that are specified in configuration files
  steampipe plugin install

  # Install a common plugin (turbot/aws)
  steampipe plugin install aws

  # Install a specific plugin version
  steampipe plugin install turbot/azure@0.1.0

  # Hide progress bars during installation
  steampipe plugin install --progress=false aws

  # Skip creation of default plugin config file
  steampipe plugin install --skip-config aws`,
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
registry is hub.steampipe.io, default org is turbot and default version
is latest. The name is a required argument.

Examples:

  # Update all plugins to their latest available version
  steampipe plugin update --all

  # Update a common plugin (turbot/aws)
  steampipe plugin update aws

  # Hide progress bars during update
  steampipe plugin update --progress=false aws`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddBoolFlag(pconstants.ArgAll, false, "Update all plugins to its latest available version").
		AddBoolFlag(pconstants.ArgProgress, true, "Display installation progress").
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin update", cmdconfig.FlagOptions.WithShortHand("h"))

	return cmd
}

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
  steampipe plugin list

  # List plugins that have updates available
  steampipe plugin list --outdated

  # List plugins output in json
  steampipe plugin list --output json`,
	}

	cmdconfig.
		OnCmd(cmd).
		AddBoolFlag("outdated", false, "Check each plugin in the list for updates").
		AddStringFlag(pconstants.ArgOutput, "table", "Output format: table or json").
		AddBoolFlag(pconstants.ArgHelp, false, "Help for plugin list", cmdconfig.FlagOptions.WithShortHand("h"))
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
  steampipe plugin uninstall aws

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
	"Updating Steampipe",
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
	// - ghcr.io/turbot/steampipe/plugins/turbot/aws:1.0.0
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
	fmt.Println()
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
	fmt.Println()
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
	// - ghcr.io/turbot/steampipe/plugins/turbot/aws:1.0.0
	plugins, err := resolveUpdatePluginsFromArgs(args)
	showProgress := viper.GetBool(pconstants.ArgProgress)

	if err != nil {
		fmt.Println()
		error_helpers.ShowError(ctx, err)
		fmt.Println()
		cmd.Help()
		fmt.Println()
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		return
	}

	if len(plugins) > 0 && !(cmdconfig.Viper().GetBool(pconstants.ArgAll)) && plugins[0] == pconstants.ArgAll {
		// improve the response to wrong argument "steampipe plugin update all"
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

	// retrieve the plugin version data from steampipe config
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
		bar.Set(len(pluginInstallSteps))
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
	docURL := fmt.Sprintf("https://hub.steampipe.io/plugins/%s/%s", org, name)
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
	// TODO implement
	// setup a cancel context and start cancel handler
	//ctx, cancel := context.WithCancel(cmd.Context())
	//contexthelpers.StartCancelHandler(cancel)
	//outputFormat := viper.GetString(pconstants.ArgOutput)
	//
	//utils.LogTime("runPluginListCmd list")
	//defer func() {
	//	utils.LogTime("runPluginListCmd end")
	//	if r := recover(); r != nil {
	//		error_helpers.ShowError(ctx, helpers.ToError(r))
	//		exitCode = pconstants.ExitCodeUnknownErrorPanic
	//	}
	//}()
	//
	//pluginList, failedPluginMap, missingPluginMap, res := getPluginList(ctx)
	//if res.Error != nil {
	//	error_helpers.ShowErrorWithMessage(ctx, res.Error, "plugin listing failed")
	//	exitCode = pconstants.ExitCodePluginListFailure
	//	return
	//}

	//err := showPluginListOutput(pluginList, failedPluginMap, missingPluginMap, res, outputFormat)
	//if err != nil {
	//	error_helpers.ShowError(ctx, err)
	//}

}

//func showPluginListOutput(pluginList []plugin.PluginListItem, failedPluginMap, missingPluginMap map[string][]plugin.PluginConnection, res perror_helpers.ErrorAndWarnings, outputFormat string) error {
//	switch outputFormat {
//	case "table":
//		return showPluginListAsTable(pluginList, failedPluginMap, missingPluginMap, res)
//	case "json":
//		return showPluginListAsJSON(pluginList, failedPluginMap, missingPluginMap, res)
//	default:
//		return errors.New("invalid output format")
//	}
//}
//
//func showPluginListAsTable(pluginList []plugin.PluginListItem, failedPluginMap, missingPluginMap map[string][]plugin.PluginConnection, res perror_helpers.ErrorAndWarnings) error {
//	headers := []string{"Installed", "Version", "Connections"}
//	var rows [][]string
//	// List installed plugins in a table
//	if len(pluginList) != 0 {
//		for _, item := range pluginList {
//			rows = append(rows, []string{item.Name, item.Version.String(), strings.Join(item.Connections, ",")})
//		}
//	} else {
//		rows = append(rows, []string{"", "", ""})
//	}
//	pplugin.ShowWrappedTable(headers, rows, &pplugin.ShowWrappedTableOptions{AutoMerge: false})
//	fmt.Printf("\n")
//
//	// List failed/missing plugins in a separate table
//	if len(failedPluginMap)+len(missingPluginMap) != 0 {
//		headers := []string{"Failed", "Connections", "Reason"}
//		var conns []string
//		var missingRows [][]string
//
//		// failed plugins
//		for p, item := range failedPluginMap {
//			for _, conn := range item {
//				conns = append(conns, conn.GetName())
//			}
//			missingRows = append(missingRows, []string{p, strings.Join(conns, ","), pconstants.ConnectionErrorPluginFailedToStart})
//			conns = []string{}
//		}
//
//		// missing plugins
//		for p, item := range missingPluginMap {
//			for _, conn := range item {
//				conns = append(conns, conn.GetName())
//			}
//			missingRows = append(missingRows, []string{p, strings.Join(conns, ","), pconstants.InstallMessagePluginNotInstalled})
//			conns = []string{}
//		}
//
//		pplugin.ShowWrappedTable(headers, missingRows, &pplugin.ShowWrappedTableOptions{AutoMerge: false})
//		fmt.Println()
//	}
//
//	if len(res.Warnings) > 0 {
//		fmt.Println()
//		res.ShowWarnings()
//		fmt.Printf("\n")
//	}
//	return nil
//}
//
//func showPluginListAsJSON(pluginList []plugin.PluginListItem, failedPluginMap, missingPluginMap map[string][]plugin.PluginConnection, res perror_helpers.ErrorAndWarnings) error {
//	output := pluginJsonOutput{}
//
//	for _, item := range pluginList {
//		installed := installedPlugin{
//			Name:        item.Name,
//			Version:     item.Version.String(),
//			Connections: item.Connections,
//		}
//		output.Installed = append(output.Installed, installed)
//	}
//
//	for p, item := range failedPluginMap {
//		connections := make([]string, len(item))
//		for i, conn := range item {
//			connections[i] = conn.GetName()
//		}
//		failed := failedPlugin{
//			Name:        p,
//			Connections: connections,
//			Reason:      pconstants.ConnectionErrorPluginFailedToStart,
//		}
//		output.Failed = append(output.Failed, failed)
//	}
//
//	for p, item := range missingPluginMap {
//		connections := make([]string, len(item))
//		for i, conn := range item {
//			connections[i] = conn.GetName()
//		}
//		missing := failedPlugin{
//			Name:        p,
//			Connections: connections,
//			Reason:      pconstants.InstallMessagePluginNotInstalled,
//		}
//		output.Failed = append(output.Failed, missing)
//	}
//
//	if len(res.Warnings) > 0 {
//		output.Warnings = res.Warnings
//	}
//
//	jsonOutput, err := json.MarshalIndent(output, "", "  ")
//	if err != nil {
//		return err
//	}
//	fmt.Println(string(jsonOutput))
//	fmt.Println()
//	return nil
//}

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
		cmd.Help()
		fmt.Println()
		exitCode = pconstants.ExitCodeInsufficientOrWrongInputs
		return
	}

	//connectionMap, _, _, res := getPluginConnectionMap(ctx)
	//if res.Error != nil {
	//	error_helpers.ShowError(ctx, res.Error)
	//	exitCode = pconstants.ExitCodePluginListFailure
	//	return
	//}

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
//func getPluginList(ctx context.Context) (pluginList []plugin.PluginListItem) {
//	statushooks.Show(ctx)
//	defer statushooks.Done(ctx)
//
//	//// get the maps of available and failed/missing plugins
//	//pluginConnectionMap, failedPluginMap, missingPluginMap, res := getPluginConnectionMap(ctx)
//	//if res.Error != nil {
//	//	return nil, nil, nil, res
//	//}
//
//	//// TODO do we really need to look at installed plugins - can't we just use the plugin connection map
//	//// get a list of the installed plugins by inspecting the install location
//	//// pass pluginConnectionMap so we can populate the connections for each plugin
//	//pluginList, err := plugin.List(ctx, pluginConnectionMap, nil)
//	//if err != nil {
//	//	res.Error = err
//	//	return nil, nil, nil, res
//	//}
//	//
//	//// remove the failed plugins from `list` since we don't want them in the installed table
//	//for pluginName := range failedPluginMap {
//	//	for i := 0; i < len(pluginList); i++ {
//	//		if pluginList[i].Name == pluginName {
//	//			pluginList = append(pluginList[:i], pluginList[i+1:]...)
//	//			i-- // Decrement the loop index since we just removed an element
//	//		}
//	//	}
//	//}
//	//return pluginList, failedPluginMap, missingPluginMap, res
//
//	// TODO IMPLEMENT
//
//	return nil, nil, nil, res
//}
//
//func getPluginConnectionMap(ctx context.Context) (pluginConnectionMap, failedPluginMap, missingPluginMap map[string][]plugin.PluginConnection, res perror_helpers.ErrorAndWarnings) {
//	utils.LogTime("cmd.getPluginConnectionMap start")
//	defer utils.LogTime("cmd.getPluginConnectionMap end")
//
//	statushooks.SetStatus(ctx, "Fetching connection map")
//
//	res = perror_helpers.ErrorAndWarnings{}
//
//	connectionStateMap, stateRes := getConnectionState(ctx)
//	res.Merge(stateRes)
//	if res.Error != nil {
//		return nil, nil, nil, res
//	}
//
//	// create the map of failed/missing plugins and available/loaded plugins
//	failedPluginMap = map[string][]plugin.PluginConnection{}
//	missingPluginMap = map[string][]plugin.PluginConnection{}
//	pluginConnectionMap = make(map[string][]plugin.PluginConnection)
//
//	for _, state := range connectionStateMap {
//		connection, ok := config.GlobalConfig.Connections[state.ConnectionName]
//		if !ok {
//			continue
//		}
//
//		if state.State == pconstants.ConnectionStateError && state.Error() == pconstants.ConnectionErrorPluginFailedToStart {
//			failedPluginMap[state.Plugin] = append(failedPluginMap[state.Plugin], connection)
//		} else if state.State == pconstants.ConnectionStateError && state.Error() == pconstants.ConnectionErrorPluginNotInstalled {
//			missingPluginMap[state.Plugin] = append(missingPluginMap[state.Plugin], connection)
//		}
//
//		pluginConnectionMap[state.Plugin] = append(pluginConnectionMap[state.Plugin], connection)
//	}
//
//	return pluginConnectionMap, failedPluginMap, missingPluginMap, res
//}
//
//// load the connection state, waiting until all connections are loaded
//func getConnectionState(ctx context.Context) (steampipeconfig.ConnectionStateMap, perror_helpers.ErrorAndWarnings) {
//	utils.LogTime("cmd.getConnectionState start")
//	defer utils.LogTime("cmd.getConnectionState end")
//
//	// start service
//	client, res := db_local.GetLocalClient(ctx, pconstants.InvokerPlugin, nil)
//	if res.Error != nil {
//		return nil, res
//	}
//	defer client.Close(ctx)
//
//	conn, err := client.AcquireManagementConnection(ctx)
//	if err != nil {
//		res.Error = err
//		return nil, res
//	}
//	defer conn.Release()
//
//	// load connection state
//	statushooks.SetStatus(ctx, "Loading connection state")
//	connectionStateMap, err := steampipeconfig.LoadConnectionState(ctx, conn.Conn(), steampipeconfig.WithWaitUntilReady())
//	if err != nil {
//		res.Error = err
//		return nil, res
//	}
//
//	return connectionStateMap, res
//}

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
