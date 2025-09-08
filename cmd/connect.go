package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/exp/maps"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	"github.com/turbot/pipe-fittings/v2/connection"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/pipe-fittings/v2/parse"
	localcmdconfig "github.com/turbot/tailpipe/internal/cmdconfig"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/parquet"
)

// variable used to assign the output mode flag
var connectOutputMode = constants.ConnectOutputModeText

func connectCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "connect [flags]",
		Args:  cobra.ArbitraryArgs,
		Run:   runConnectCmd,
		Short: "Return the path of SQL script to initialise DuckDB to use the tailpipe database",
		Long: `Return the path of SQL script to initialise DuckDB to use the tailpipe database.

The generated SQL script contains:
- DuckDB extension installations (sqlite, ducklake)
- Database attachment configuration
- View definitions with optional filters

Examples:
  # Basic usage - generate init script
  tailpipe connect

  # Filter by time range
  tailpipe connect --from "2024-01-01" --to "2024-01-31"

  # Filter by specific partitions
  tailpipe connect --partition "aws_cloudtrail_log.recent"

  # Filter by indexes with wildcards
  tailpipe connect --index "prod-*" --index "staging"

  # Combine multiple filters
  tailpipe connect --from "T-7d" --partition "aws.*" --index "prod-*"

  # Output as JSON
  tailpipe connect --output json

Time formats supported:
  - ISO 8601 date: 2024-01-01
  - ISO 8601 datetime: 2024-01-01T15:04:05
  - RFC 3339 with timezone: 2024-01-01T15:04:05Z
  - Relative time: T-7d, T-2Y, T-10m, T-180d

The generated script can be used with DuckDB:
  duckdb -init /path/to/generated/script.sql`,
	}

	// args `from` and `to` accept:
	// - ISO 8601 date (2024-01-01)
	// - ISO 8601 datetime (2006-01-02T15:04:05)
	// - ISO 8601 datetime with ms (2006-01-02T15:04:05.000)
	// - RFC 3339 datetime with timezone (2006-01-02T15:04:05Z07:00)
	// - relative time formats (T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)

	cmdconfig.OnCmd(cmd).
		AddStringFlag(pconstants.ArgFrom, "", "Specify the start time").
		AddStringFlag(pconstants.ArgTo, "", "Specify the end time").
		AddStringSliceFlag(pconstants.ArgIndex, nil, "Specify the index to use").
		AddStringSliceFlag(pconstants.ArgPartition, nil, "Specify the partition to use").
		AddVarFlag(enumflag.New(&connectOutputMode, pconstants.ArgOutput, constants.ConnectOutputModeIds, enumflag.EnumCaseInsensitive),
			pconstants.ArgOutput,
			fmt.Sprintf("Output format; one of: %s", strings.Join(constants.FlagValues(constants.PluginOutputModeIds), ", ")))

	return cmd
}

func runConnectCmd(cmd *cobra.Command, _ []string) {
	var err error
	var initFilePath string
	ctx := cmd.Context()

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		setExitCodeForConnectError(err)
		displayOutput(ctx, initFilePath, err)
	}()

	// if diagnostic mode is set, print out config and return
	if _, ok := os.LookupEnv(constants.EnvConfigDump); ok {
		localcmdconfig.DisplayConfig()
		return
	}

	initFilePath, err = generateInitFile(ctx)

	// we are done - the defer block will print either the filepath (if successful) or the error (if not)

}

func generateInitFile(ctx context.Context) (string, error) {
	// cleanup the old db files if not in use
	err := cleanupOldInitFiles()
	if err != nil {
		return "", err
	}

	// generate a filename to write the init sql to, inside the data dir
	initFilePath := generateInitFilename(config.GlobalWorkspaceProfile.GetDataDir())

	// get the sql to attach readonly to the database
	commands := database.GetDucklakeInitCommands(true)

	// build the filters from the to, from and index args
	// these will be used in the view definitions
	filters, err := getFilters()
	if err != nil {
		return "", fmt.Errorf("error building filters: %w", err)
	}

	// create a temporary duckdb instance pass to get the view definitions
	db, err := database.NewDuckDb(database.WithDuckLakeReadonly())
	if err != nil {
		return "", fmt.Errorf("failed to create duckdb: %w", err)
	}
	defer db.Close()

	// get the view creation SQL, with filters applied
	viewCommands, err := database.GetCreateViewsSql(ctx, db, filters...)
	if err != nil {
		return "", err
	}
	commands = append(commands, viewCommands...)

	// now build a string
	var str strings.Builder
	for _, cmd := range commands {
		str.WriteString(fmt.Sprintf("-- %s\n%s;\n\n", cmd.Description, cmd.Command))
	}
	// write out the init file
	err = os.WriteFile(initFilePath, []byte(str.String()), 0644) //nolint:gosec // we want the init file to be readable
	if err != nil {
		return "", fmt.Errorf("failed to write init file: %w", err)
	}
	return initFilePath, err
}

// cleanupOldInitFiles deletes old db init files (older than a day)
func cleanupOldInitFiles() error {
	baseDir := pfilepaths.GetDataDir()
	log.Printf("[INFO] Cleaning up old init files in %s\n", baseDir)
	cutoffTime := time.Now().Add(-constants.InitFileMaxAge) // Files older than 1 day

	// The baseDir ("$TAILPIPE_INSTALL_DIR/data") is expected to have subdirectories for different workspace
	// profiles(default, work etc). Each subdirectory may contain multiple .db files.
	// Example structure:
	// data/
	// ├── default/
	// │   ├── tailpipe_init_20250115182129.sql
	// │   ├── tailpipe_init_20250115193816.sql
	// │   └── ...
	// ├── work/
	// │   ├── tailpipe_init_20250115182129.sql
	// │   ├── tailpipe_init_20250115193816.sql
	// │   └── ...
	// So we traverse all these subdirectories for each workspace and process the relevant files.
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %v", path, err)
		}

		// skip directories and non-`.sql` files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}

		// only process `tailpipe_init_*.sql` files
		if !strings.HasPrefix(info.Name(), "tailpipe_init_") {
			return nil
		}

		// check if the file is older than the cutoff time
		if info.ModTime().After(cutoffTime) {
			log.Printf("[DEBUG] Skipping deleting file %s(%s) as it is not older than %s\n", path, info.ModTime().String(), cutoffTime)
			return nil
		}

		err = os.Remove(path)
		if err != nil {
			log.Printf("[INFO] Failed to delete db file %s: %v", path, err)
		} else {
			log.Printf("[DEBUG] Cleaned up old unused db file: %s\n", path)
		}

		return nil
	})

	if err != nil {
		return err
	}
	return nil

}

func displayOutput(ctx context.Context, initFilePath string, err error) {
	switch viper.GetString(pconstants.ArgOutput) {
	case pconstants.OutputFormatText:
		if err == nil {
			// output the filepath
			fmt.Println(initFilePath) //nolint:forbidigo // ui output
		} else {
			error_helpers.ShowError(ctx, err)
		}
	case pconstants.OutputFormatJSON:
		res := connection.TailpipeConnectResponse{
			InitScriptPath: initFilePath,
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

// getFilters builds a set of SQL filters based on the provided command line args
// supported args are `from`, `to`, `partition` and `index`
func getFilters() ([]string, error) {
	var result []string
	if viper.IsSet(pconstants.ArgFrom) {
		from := viper.GetString(pconstants.ArgFrom)
		// parse the string as time.Time
		// arg `from` accepts ISO 8601 date(2024-01-01), ISO 8601 datetime(2006-01-02T15:04:05), ISO 8601 datetime with ms(2006-01-02T15:04:05.000),
		// RFC 3339 datetime with timezone(2006-01-02T15:04:05Z07:00) and relative time formats(T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)
		t, err := parse.ParseTime(from, time.Now())
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'from': %s", from)
		}
		// format as SQL timestamp
		fromTimestamp := t.Format(time.DateTime)
		result = append(result, fmt.Sprintf("tp_timestamp >= timestamp '%s'", fromTimestamp))
	}
	if viper.IsSet(pconstants.ArgTo) {
		to := viper.GetString(pconstants.ArgTo)
		// parse the string as time.Time
		// arg `to` accepts ISO 8601 date(2024-01-01), ISO 8601 datetime(2006-01-02T15:04:05), ISO 8601 datetime with ms(2006-01-02T15:04:05.000),
		// RFC 3339 datetime with timezone(2006-01-02T15:04:05Z07:00) and relative time formats(T-2Y, T-10m, T-10W, T-180d, T-9H, T-10M)
		t, err := parse.ParseTime(to, time.Now())
		if err != nil {
			return nil, fmt.Errorf("invalid date format for 'to': %s", to)
		}
		// format as SQL timestamp
		toTimestamp := t.Format(time.DateTime)
		result = append(result, fmt.Sprintf("tp_timestamp <= timestamp '%s'", toTimestamp))
	}
	if viper.IsSet(pconstants.ArgPartition) {
		// we have loaded tailpipe config by this time
		availablePartitions := config.GlobalConfig.Partitions
		partitionArgs := viper.GetStringSlice(pconstants.ArgPartition)
		// get the SQL filters from the provided partition
		sqlFilters, err := getPartitionSqlFilters(partitionArgs, maps.Keys(availablePartitions))
		if err != nil {
			return nil, err
		}
		result = append(result, sqlFilters)
	}
	if viper.IsSet(pconstants.ArgIndex) {
		indexArgs := viper.GetStringSlice(pconstants.ArgIndex)
		// get the SQL filters from the provided index
		sqlFilters, err := getIndexSqlFilters(indexArgs)
		if err != nil {
			return nil, err
		}
		result = append(result, sqlFilters)
	}
	return result, nil
}

// getPartitionSqlFilters builds SQL filters for the provided partition args
func getPartitionSqlFilters(partitionArgs []string, availablePartitions []string) (string, error) {
	// Get table and partition patterns using GetPartitionPatternsForArgs
	patterns, err := parquet.GetPartitionPatternsForArgs(availablePartitions, partitionArgs...)
	if err != nil {
		return "", fmt.Errorf("error processing partition args: %w", err)
	}

	// Handle the case when patterns are empty
	if len(patterns) == 0 {
		return "", nil
	}

	// Replace wildcards from '*' to '%' for SQL compatibility
	sqlPatterns := replaceWildcards(patterns)

	var conditions []string

	for i := 0; i < len(sqlPatterns); i++ {
		table := sqlPatterns[i].Table
		partition := sqlPatterns[i].Partition

		var tableCondition, partitionCondition string

		// If there is no wildcard, use '=' instead of like
		if table == "%" {
			// Skip table condition if full wildcard
			tableCondition = ""
		} else if strings.Contains(table, "%") {
			tableCondition = fmt.Sprintf("tp_table like '%s'", table)
		} else {
			tableCondition = fmt.Sprintf("tp_table = '%s'", table)
		}

		if partition == "%" {
			// Skip partition condition if full wildcard
			partitionCondition = ""
		} else if strings.Contains(partition, "%") {
			partitionCondition = fmt.Sprintf("tp_partition like '%s'", partition)
		} else {
			partitionCondition = fmt.Sprintf("tp_partition = '%s'", partition)
		}

		// Remove empty conditions and combine valid ones
		if tableCondition != "" && partitionCondition != "" {
			conditions = append(conditions, fmt.Sprintf("(%s and %s)", tableCondition, partitionCondition))
		} else if tableCondition != "" {
			conditions = append(conditions, tableCondition)
		} else if partitionCondition != "" {
			conditions = append(conditions, partitionCondition)
		}
	}

	// Combine all conditions with OR
	sqlFilters := strings.Join(conditions, " OR ")

	return sqlFilters, nil
}

// getIndexSqlFilters builds SQL filters for the provided index args
func getIndexSqlFilters(indexArgs []string) (string, error) {
	// Return empty if no indexes provided
	if len(indexArgs) == 0 {
		return "", nil
	}

	// Build SQL filter based on whether wildcards are present
	var conditions []string
	for _, index := range indexArgs {
		if index == "*" {
			// Skip index condition if full wildcard
			conditions = append(conditions, "")
		} else if strings.Contains(index, "*") {
			// Replace '*' wildcard with '%' for SQL like compatibility
			index = strings.ReplaceAll(index, "*", "%")
			conditions = append(conditions, fmt.Sprintf("cast(tp_index as varchar) like '%s'", index))
		} else {
			// Exact match using '='
			conditions = append(conditions, fmt.Sprintf("tp_index = '%s'", index))
		}
	}

	// Combine all conditions with OR
	sqlFilter := strings.Join(conditions, " OR ")

	return sqlFilter, nil
}

// convert partition patterns with '*' wildcards to SQL '%' wildcards
func replaceWildcards(patterns []*parquet.PartitionPattern) []*parquet.PartitionPattern {
	updatedPatterns := make([]*parquet.PartitionPattern, len(patterns))

	for i, p := range patterns {
		updatedPatterns[i] = &parquet.PartitionPattern{
			Table:     strings.ReplaceAll(p.Table, "*", "%"),
			Partition: strings.ReplaceAll(p.Partition, "*", "%")}
	}
	return updatedPatterns

}

func setExitCodeForConnectError(err error) {
	// if exit code already set, leave as is
	// NOTE: DO NOT set exit code if the output format is JSON
	if exitCode != 0 || err == nil || viper.GetString(pconstants.ArgOutput) == pconstants.OutputFormatJSON {
		return
	}

	exitCode = 1
}

// generateInitFilename generates a temporary filename with a timestamp
func generateInitFilename(dataDir string) string {
	timestamp := time.Now().Format("20060102150405") // e.g., 20241031103000
	return filepath.Join(dataDir, fmt.Sprintf("tailpipe_init_%s.sql", timestamp))
}
