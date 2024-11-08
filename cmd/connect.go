package cmd

import (
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/cmdconfig"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/database"
	"os"
	"path/filepath"
	"time"
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

func runConnectCmd(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
			error_helpers.ShowError(ctx, err)
		}
		setExitCodeForConnectError(err)
	}()

	databaseFilePath := generateTempDBFilename(config.GlobalWorkspaceProfile.GetDataDir())

	//
	// Open a DuckDB connection (creates the file if it doesn't exist)
	var db *sql.DB
	db, err = sql.Open("duckdb", databaseFilePath)
	if err != nil {
		return
	}

	defer db.Close()

	// identify tables by listing files in the data directory
	tables, err := getDirNames(config.GlobalWorkspaceProfile.GetDataDir())
	if err != nil {
		return
	}
	// create a view for each table
	// first build the filters
	filters, err := getFilters()
	if err != nil {
		return
	}

	for _, table := range tables {
		// create a view for the table
		err = database.AddTableView(ctx, table, db, filters...)
		if err != nil {
			return
		}
	}

	fmt.Println(fmt.Sprintf("duckdb://%s", databaseFilePath))
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
		from = fmt.Sprintf("%d", t.UnixNano()/1000000)
		result = append(result, fmt.Sprintf("tp_timestamp >= %s", from))
	}
	if viper.IsSet(pconstants.ArgTo) {
		to := viper.GetString(pconstants.ArgTo)
		// verify this is a valid date
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'to': %s", to)
		}

		//convert to unix milliseconds
		to = fmt.Sprintf("%d", t.UnixNano()/1000000)
		result = append(result, fmt.Sprintf("tp_timestamp <= %s", to))

	}
	return result, nil
}

func getDirNames(folderPath string) ([]string, error) {
	var dirNames []string

	// Read the directory contents
	files, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	// Loop through the contents and add directories to dirNames
	for _, file := range files {
		if file.IsDir() {
			dirNames = append(dirNames, file.Name())
		}
	}

	return dirNames, nil
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
