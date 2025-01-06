package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/connection"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
)

// variable used to assign the output mode flag
var connectOutputMode = constants.ConnectOutputModeText

func connectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [flags]",
		Args:  cobra.ArbitraryArgs,
		Run:   runConnectCmd,
		Short: "return a connection string for a database with a schema determined by the provided parameters",
		Long:  `return a connection string for a database with a schema determined by the provided parameters.`,
	}

	cmdconfig.OnCmd(cmd).
		AddStringFlag(pconstants.ArgFrom, "", "Specify the start time").
		AddStringFlag(pconstants.ArgTo, "", "Specify the end time").
		AddVarFlag(enumflag.New(&connectOutputMode, pconstants.ArgOutput, constants.ConnectOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", ")))

	return cmd
}

func runConnectCmd(cmd *cobra.Command, _ []string) {
	var err error
	var databaseFilePath string
	ctx := cmd.Context()

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		setExitCodeForConnectError(err)
		displayOutput(ctx, databaseFilePath, err)
	}()

	databaseFilePath, err = generateDbFile(ctx)

	// we are done - the defer block will print either the filepath (if successful) or the error (if not)
}

func generateDbFile(ctx context.Context) (string, error) {
	databaseFilePath := generateTempDBFilename(config.GlobalWorkspaceProfile.GetDataDir())

	// first build the filters
	filters, err := getFilters()
	if err != nil {
		return "", fmt.Errorf("error building filters: %w", err)
	}

	// if there are no filters, just copy the db file
	if len(filters) == 0 {
		err = copyDBFile(filepaths.TailpipeDbFilePath(), databaseFilePath)
		return databaseFilePath, err
	}

	// Open a DuckDB connection (creates the file if it doesn't exist)
	var db *sql.DB
	db, err = sql.Open("duckdb", databaseFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	err = database.AddTableViews(ctx, db, filters...)
	return databaseFilePath, err
}

func displayOutput(ctx context.Context, databaseFilePath string, err error) {
	switch viper.GetString(pconstants.ArgOutput) {
	case pconstants.OutputFormatText:
		if err == nil {
			// output the filepath
			fmt.Println(databaseFilePath) //nolint:forbidigo // ui output
		} else {
			error_helpers.ShowError(ctx, err)
		}
	case pconstants.OutputFormatJSON:
		res := connection.TailpipeConnectResponse{
			DatabaseFilepath: databaseFilePath,
		}
		if err != nil {
			res.Error = err.Error()
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

func getFilters() ([]string, error) {
	var result []string
	if viper.IsSet(pconstants.ArgFrom) {
		from := viper.GetString(pconstants.ArgFrom)
		// verify this is a valid date
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'from': %s", from)
		}
		// format as SQL timestamp
		fromDate := t.Format("2006-01-02")
		fromTimestamp := t.Format("2006-01-02 15:04:05")
		result = append(result, fmt.Sprintf("tp_date >= DATE '%s' AND tp_timestamp >= TIMESTAMP '%s'", fromDate, fromTimestamp))
	}
	if viper.IsSet(pconstants.ArgTo) {
		to := viper.GetString(pconstants.ArgTo)
		// verify this is a valid date
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'to': %s", to)
		}
		// format as SQL timestamp
		toDate := t.Format("2006-01-02")
		toTimestamp := t.Format("2006-01-02 15:04:05")
		result = append(result, fmt.Sprintf("tp_date <= DATE '%s' AND tp_timestamp <= TIMESTAMP '%s'", toDate, toTimestamp))
	}
	return result, nil
}

// generateTempDBFilename generates a temporary filename with a timestamp
func generateTempDBFilename(dataDir string) string {
	timestamp := time.Now().Format("20060102150405") // e.g., 20241031103000
	return filepath.Join(dataDir, fmt.Sprintf("tailpipe_%s.db", timestamp))
}

func setExitCodeForConnectError(err error) {
	// if exit code already set, leave as is
	// NOTE: DO NOT set exit code if the output format is JSON
	if exitCode != 0 || err == nil || viper.GetString(pconstants.ArgOutput) == pconstants.OutputFormatJSON {
		return
	}

	exitCode = 1
}

// copyDBFile copies the source database file to the destination
func copyDBFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}
