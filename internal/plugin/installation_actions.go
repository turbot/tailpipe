package plugin

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/turbot/go-kit/files"
	"github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/ociinstaller"
	"github.com/turbot/pipe-fittings/v2/plugin"
	"github.com/turbot/pipe-fittings/v2/statushooks"
	"github.com/turbot/pipe-fittings/v2/versionfile"
)

// Remove removes an installed plugin
func Remove(ctx context.Context, image string) (*PluginRemoveReport, error) {
	statushooks.SetStatus(ctx, fmt.Sprintf("Removing plugin %s", image))

	imageRef := ociinstaller.NewImageRef(image)
	fullPluginName := imageRef.DisplayImageRef()

	installedTo := filepath.Join(filepaths.EnsurePluginDir(), filepath.FromSlash(fullPluginName))
	_, err := os.Stat(installedTo)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("plugin '%s' not found", image)
	}

	// remove from file system
	err = os.RemoveAll(installedTo)
	if err != nil {
		return nil, err
	}

	// update the version file
	v, err := versionfile.LoadPluginVersionFile(ctx)
	if err != nil {
		return nil, err
	}
	delete(v.Plugins, fullPluginName)
	err = v.Save()

	return &PluginRemoveReport{Image: imageRef}, err
}

// Install installs a plugin in the local file system
func Install(ctx context.Context, plugin plugin.ResolvedPluginVersion, sub chan struct{}, baseImageRef string, mediaTypesProvider ociinstaller.MediaTypeProvider) (*ociinstaller.OciImage[*ociinstaller.PluginImage, *ociinstaller.PluginImageConfig], error) {
	// set options for the plugin install
	opts := []ociinstaller.PluginInstallOption{
		ociinstaller.WithSkipConfig(viper.GetBool(constants.ArgSkipConfig)),
		// pass metadata function to populate the plugin metadata in the versions file
		ociinstaller.WithGetMetadataFunc(getPluginMetadata),
	}

	// Note: we pass the plugin info as strings here rather than passing the ResolvedPluginVersion struct as that causes circular dependency
	image, err := ociinstaller.InstallPlugin(ctx, plugin.GetVersionTag(), plugin.Constraint, sub, baseImageRef, mediaTypesProvider, opts...)
	return image, err
}

// PluginNameVersion is a struct representing an item in the list of plugins
type PluginNameVersion struct {
	Name    string
	Version *plugin.PluginVersionString
}

// List returns all installed plugins by finding plugin binaries in the plugin directory
func List(ctx context.Context, pluginVersions map[string]*versionfile.InstalledVersion, fileNameFilter *string) ([]PluginNameVersion, error) {
	var items []PluginNameVersion
	filter := "**/*.plugin"
	if fileNameFilter != nil {
		filter = fmt.Sprintf("**/*%s.plugin", *fileNameFilter)
	}

	pluginBinaries, err := files.ListFilesWithContext(ctx, filepaths.EnsurePluginDir(), &files.ListOptions{
		Include: []string{filter},
		Flags:   files.AllRecursive,
	})
	if err != nil {
		return nil, err
	}

	// we have the plugin binary paths
	for _, pluginBinary := range pluginBinaries {
		parent := filepath.Dir(pluginBinary)
		fullPluginName, err := filepath.Rel(filepaths.EnsurePluginDir(), parent)
		if err != nil {
			return nil, err
		}
		// for local plugin
		item := PluginNameVersion{
			Name:    fullPluginName,
			Version: plugin.LocalPluginVersionString(),
		}
		// check if this plugin is recorded in plugin versions
		installation := pluginVersions[fullPluginName]

		// if not a local plugin, get the semver version
		if !detectLocalPlugin(installation, pluginBinary) {
			item.Version, err = plugin.NewPluginVersionString(installation.Version)
			if err != nil {
				return nil, fmt.Errorf("could not evaluate plugin version %s: %w", installation.Version, err)
			}
		}

		items = append(items, item)
	}

	return items, nil
}

// detectLocalPlugin returns true if the modTime of the `pluginBinary` is after the installation date as recorded in the installation data
// this may happen when a plugin is installed from the registry, but is then compiled from source
func detectLocalPlugin(installation *versionfile.InstalledVersion, pluginBinary string) bool {
	// TODO this should no longer be necessary as we now have a "local" version number in the versions file?
	if installation == nil {
		return true
	}
	installDate, err := time.Parse(time.RFC3339, installation.InstallDate)
	if err != nil {
		log.Printf("[WARN] could not parse install date for %s: %s", installation.Name, installation.InstallDate)
		return false
	}

	// truncate to second
	// otherwise, comparisons may get skewed because of the
	// underlying monotonic clock
	installDate = installDate.Truncate(time.Second)

	// get the modtime of the plugin binary
	stat, err := os.Lstat(pluginBinary)
	if err != nil {
		log.Printf("[WARN] could not parse install date for %s: %s", installation.Name, installation.InstallDate)
		return false
	}
	modTime := stat.ModTime().
		// truncate to second
		// otherwise, comparisons may get skewed because of the
		// underlying monotonic clock
		Truncate(time.Second)

	return installDate.Before(modTime)
}

// getPluginMetadata returns the metadata for a given plugin, primarily the tables, sources and formats for usage in writing them to the InstalledVersionFile.
// This is passed into Install using the ociinstaller.WithGetMetadataFunc option.
func getPluginMetadata(ctx context.Context, pluginName string) (map[string][]string, error) {
	manager := NewPluginManager()
	defer manager.Close()

	describeResponse, err := manager.Describe(ctx, pluginName)
	if err != nil {
		return nil, err
	}

	return describeResponse.AsMetadataMap(), nil
}
