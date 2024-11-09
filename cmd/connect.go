package cmd

import (
	"database/sql"
	"fmt"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/config"
)

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
		AddStringFlag(pconstants.ArgTo, "", "Specify the end time")

	return cmd
}

func runConnectCmd(cmd *cobra.Command, _ []string) {
	ctx := cmd.Context()
	var err error
	databaseFilePath := generateTempDBFilename(config.GlobalWorkspaceProfile.GetDataDir())

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			error_helpers.ShowError(ctx, err)
		}
		setExitCodeForConnectError(err)
		if err == nil {
			// output the filepath
			fmt.Println(databaseFilePath)
		} else {
			error_helpers.ShowError(ctx, err)
		}
	}()

	// first build the filters
	filters, err := getFilters()
	if err != nil {
		err = fmt.Errorf("error building filters: %w", err)
		return
	}

	alwaysCopy := viper.GetBool("copy")
	// if there are no filters, just copy the db file
	if len(filters) == 0 || alwaysCopy {
		err = copyDBFile(filepaths.TailpipeDbFilePath(), databaseFilePath)
	}

	if len(filters) == 0 {
		return
	}

	// Open a DuckDB connection (creates the file if it doesn't exist)
	var db *sql.DB
	db, err = sql.Open("duckdb", databaseFilePath)
	if err != nil {
		return
	}
	defer db.Close()

	err = database.AddTableViews(ctx, db, filters...)

	// we are done - the defer block will print either the filepath (if successful) or the error (if not)
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
		//convert to unix milliseconds
		fromDate := t.Format("2006-01-02")
		fromTimestamp := fmt.Sprintf("%d", t.UnixNano()/1000000)
		result = append(result, fmt.Sprintf("tp_date >= DATE '%s' AND tp_timestamp >= %s", fromDate, fromTimestamp))
	}
	if viper.IsSet(pconstants.ArgTo) {
		to := viper.GetString(pconstants.ArgTo)
		// verify this is a valid date
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'to': %s", to)
		}

		//convert to unix milliseconds
		toDate := t.Format("2006-01-02")
		toTimestamp := fmt.Sprintf("%d", t.UnixNano()/1000000)
		result = append(result, fmt.Sprintf("tp_date <= DATE '%s' AND tp_timestamp <= %s", toDate, toTimestamp))

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
	if exitCode != 0 || err == nil {
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
