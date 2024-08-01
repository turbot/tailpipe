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
	"github.com/turbot/tailpipe-plugin-sdk/artifact_source"
	"github.com/turbot/tailpipe/internal/collector"
	"github.com/turbot/tailpipe/internal/config"
	"time"
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

	// TODO #config TACTICAL
	cloudtrail_log_cfg := `
	paths = ["/Users/kai/tailpipe_data/flaws_cloudtrail_logs"]
	extensions = [".gz"]	
`
	start_time := time.Now().Add(-time.Hour * 24 * 30)
	end_time := time.Now().UTC()
	flow_log_cfg := fmt.Sprintf(`
		log_group_name = "/victor/vpc/flowlog"
		start_time = "%s"
		end_time = "%s"
		access_key = "ASIARNKUQPUTUDLXX5FT"
		secret_key = "ikekZZgcuL64Tr59jgt0GZiROsTw3GngUMVfNnfa"
		session_token = "IQoJb3JpZ2luX2VjEI///////////wEaCXVzLWVhc3QtMiJHMEUCIFzmGBMKz+AcgA8Z9htsyHFNwkUhjgSx9QErjUVWhwiGAiEAlMeQrFRTUif60Ve6RPd9DIxzn7Defmy2XGz5F81jOfgqhgMIeBADGgwwOTczNTA4NzY0NTUiDFJxH2F9GRyz5RFlDyrjAtlStUWnqoxs0jIrXeJI0UpmVLEFCsaCR2q2/6RNC3zbt+IrBEdUBEmbjakE8FSShn5LFdmuzr16LJ7mD/LbkRY4ujJ7TQB92m+J/W9rfcjvt4iOf6YqOEg4p/+qh7PnfTUrw/aJ2DvjTDL/EiVMPro+eWAu3cizmAXMDy0seHgqyCiFpbg/S0RXP/AuT58Vw7assNeTrspiNew2zxbvdpif0nq2Nqg6/IOegjn1eRrqpyRXgatwGKtPgKWgOHM+D7p9YjDh5RqiDNl3/TENcCaSyCVygRuOE4RDTVQHo+vwSLezSaikvp6/5fe8NO3nIwSJCwbTkunHJHiTcOK5TOKnYA3obROSjTiStR8s9dHsWpkRIegY4JzGmg4hLIiREnrMkW2XZ+sQd/Yc1iDzU4855mDVTecKMPuuMwm+BF8SCcO8RVLC/4/QaKPvCYzJeJoQJli0XF+zKBjXein5ebxVS2Iw0MSutQY6pgGOEDJMiKcxz83uT5+xWCkik7GsESAli/y3eXD/aKZKYvJyRxHjvfZw8aWdWIoPv+/TD83rbVECDe/OdWMzEIQb+QOYlCSYfdqB/I4DeeyPpdV6+b4Xih+wjSeD7EirpaUIrDznMZ6Qyomituq3vgRhSBUs0ei+KrmW6YxdnaEkppRqDxnEFbIsVffEHThenzfzcHmLMOWV+1g5DdG7Yf7hQFmKlURx"
`, start_time.Format(time.RFC3339), end_time.Format(time.RFC3339))

	gcp_audit_log_cfg := `
		credentials = "/Users/graza/gcp/tailpipe-creds.json"
		project = "parker-aaa"
		log_types = ["activity", "data_access", "system_event"]
		`
	nginx_access_log_cfg := `
		paths = ["/Users/graza/tailpipe_data/nginx_access_logs"]
		extensions = [".log"]
		`
	allCollections := map[string]*config.Collection{
		"aws_cloudtrail_log": {
			Type:   "aws_cloudtrail_log",
			Plugin: "aws",
			Source: config.Source{
				// NOTE we ae actually passing the artifact source type here
				Type:   artifact_source.FileSystemSourceIdentifier,
				Config: []byte(cloudtrail_log_cfg),
			},
		},
		"aws_vpc_flow_log": {
			Type:   "aws_vpc_flow_log",
			Plugin: "aws",
			Source: config.Source{
				// NOTE we ae actually passing the artifact source type here
				Type:   artifact_source.AWSCloudwatchSourceIdentifier,
				Config: []byte(flow_log_cfg),
			},
		},
		"pipes_audit_log": {
			Type:   "pipes_audit_log",
			Plugin: "pipes",
		},
		"gcp_audit_log": {
			Type:   "gcp_audit_log",
			Plugin: "gcp",
			Source: config.Source{
				Type:   "gcp_audit_log_api",
				Config: []byte(gcp_audit_log_cfg),
			},
		},
		"nginx_access_log": {
			Type:   "nginx_access_log",
			Plugin: "nginx",
			Config: []byte(`log_format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent \"$http_referer\" \"$http_user_agent\""`),
			Source: config.Source{
				Type:   artifact_source.FileSystemSourceIdentifier,
				Config: []byte(nginx_access_log_cfg),
			},
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
