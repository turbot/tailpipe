package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/pipe-fittings/v2/backend"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	"github.com/turbot/pipe-fittings/v2/connection"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"path/filepath"
	"strings"
)

// variable used to assign the output mode flag
var connectOutputMode = constants.ConnectOutputModeText

func connectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [flags]",
		Args:  cobra.ArbitraryArgs,
		Run:   runConnectCmd,
		Short: "Return a connection string for the ducklake database",
		Long:  "Return a connection string for the ducklake database.",
	}

	cmdconfig.OnCmd(cmd).
		AddStringFlag(pconstants.ArgFrom, "", "Specify the start time", cmdconfig.FlagOptions.Deprecated("'from' is not supported with ducklake")).
		AddStringFlag(pconstants.ArgTo, "", "Specify the end time", cmdconfig.FlagOptions.Deprecated("'to'' is not supported with ducklake")).
		AddStringSliceFlag(pconstants.ArgIndex, nil, "Specify the index to use", cmdconfig.FlagOptions.Deprecated("'index' is not supported with ducklake")).
		AddStringSliceFlag(pconstants.ArgPartition, nil, "Specify the partition to use", cmdconfig.FlagOptions.Deprecated("'partition' is not supported with ducklake")).
		AddVarFlag(enumflag.New(&connectOutputMode, pconstants.ArgOutput, constants.ConnectOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", ")))

	return cmd
}

func runConnectCmd(cmd *cobra.Command, _ []string) {
	ctx := cmd.Context()
	dataPath := config.GlobalWorkspaceProfile.GetDataDir()
	dbFilePath := filepath.Join(dataPath, "metadata.sqlite")

	switch viper.GetString(pconstants.ArgOutput) {
	case pconstants.OutputFormatText:
		// output the filepath
		connectionString := backend.GetDucklakeConnectionString(dbFilePath, dataPath)
		fmt.Println(connectionString) //nolint:forbidigo // ui output

	case pconstants.OutputFormatJSON:
		res := connection.TailpipeConnectResponse{
			DatabaseFilepath: dbFilePath,
			DataPath:         dataPath,
		}

		b, err := json.Marshal(res)
		if err == nil {
			fmt.Println(string(b)) //nolint:forbidigo // ui output
		} else {
			fmt.Printf(`{"error": "failed to marshal response: %s"}`, err) //nolint:forbidigo // ui output
		}

	default:
		// unexpected - cobras validation should prevent
		error_helpers.ShowError(ctx, fmt.Errorf("unsupported output format %q", viper.GetString(pconstants.ArgOutput)))
	}
}
