package cmd

import (
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
)

// TODO #errors have good think about error handling and return codes

func collectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "collect [flags]",
		TraverseChildren: true,
		Run:              runCollectCmd,
		Short:            "Collect logs from configured sources",
		Long:             `Collect logs from configured sources.`,
	}

	cmdconfig.OnCmd(cmd).
		AddStringFlag(constants.ArgConfigPath, ".", "Path to search for config files").
		AddStringSliceFlag(constants.ArgCollection, nil, "Collection(s) to collect (default is all)")

	return cmd
}

func runCollectCmd(cmd *cobra.Command, _ []string) {
	ctx := cmd.Context()

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			error_helpers.ShowError(ctx, err)
		}
		setExitCodeForCollectError(err)
	}()

	//// load config
	//config, err := localparse.LoadTailpipeConfig(viper.GetString(constants.ArgConfigPath))
	//error_helpers.FailOnError(err)
	//slog.Info("config loaded", "config", config)
	//if len(config.Collections) == 0 {
	//	error_helpers.FailOnError(fmt.Errorf("loaded config does not contain any collections"))
	//}
	//
	//// validate collection args and retrieve the collection configs
	//collections, err := getTargetCollectionConfig(config)
	//error_helpers.FailOnError(err)

	// todo tactical
	allCollections := map[string]*config.Collection{
		"aws_cloudtrail_log": {
			Type:   "aws_cloudtrail_log",
			Plugin: "aws",
		},
		"aws_flow_log": {
			Type:   "aws_flow_log",
			Plugin: "aws",
		},
		"aws_s3_access_log": {
			Type:   "pipes_audit_log",
			Plugin: "pipes",
		},
	}
	var collections []*config.Collection

	for _, c := range viper.GetStringSlice(constants.ArgCollection) {
		if col, ok := allCollections[c]; !ok {
			error_helpers.FailOnError(fmt.Errorf("config does not contain %s: %s", "collection", c))
		} else {
			collections = append(collections, col)
		}
	}

	if len(collections) == 0 {
		collections = []*config.Collection{allCollections["aws_cloudtrail_log"]}
	}
	// tactical

	// now we have the collections, we can start collecting

	// create a collector
	c, err := collector.New(ctx)
	if err != nil {
		error_helpers.FailOnError(fmt.Errorf("failed to create collector: %w", err))
	}

	var errList []error
	for _, col := range collections {
		if err := c.Collect(ctx, col); err != nil {
			errList = append(errList, err)
		}
	}
	if len(errList) > 0 {
		err = errors.Join(errList...)
		error_helpers.FailOnError(fmt.Errorf("collection error: %w", err))
	}

	// now wait for all collections to complete and close the collector
	c.Close()

	fmt.Println("collection complete")
}

//func getTargetCollectionConfig(tailpipeConfig *config.TailpipeConfig) ([]*config.Collection, error) {
//	collectionNames := viper.GetStringSlice(constants.ArgCollection)
//
//	// if no collections specified, return all
//	if len(collectionNames) == 0 {
//		return maps.Values(tailpipeConfig.Collections), nil
//	}
//
//	var collections []*config.Collection
//	var missing []string
//	for _, name := range collectionNames {
//		c := tailpipeConfig.Collections[name]
//		if c == nil {
//			missing = append(missing, name)
//		} else {
//			collections = append(collections, c)
//		}
//	}
//
//	if len(missing) > 0 {
//		return nil, fmt.Errorf("config does not contain %s: %s", utils.Pluralize("collection", len(missing)), strings.Join(missing, ", "))
//	}
//	return collections, nil
//}

func setExitCodeForCollectError(err error) {
	// if exit code already set, leave as is
	if exitCode != 0 || err == nil {
		return
	}

	// TODO #errors - assign exit codes
	exitCode = 1
}
