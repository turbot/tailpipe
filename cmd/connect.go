package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/exp/maps"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/pipe-fittings/v2/cmdconfig"
	"github.com/turbot/pipe-fittings/v2/connection"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
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
	initFilePath := generateInitFilename(config.GlobalWorkspaceProfile.GetDataDir())

	// first build the filters
	filters, err := getFilters()
	if err != nil {
		return "", fmt.Errorf("error building filters: %w", err)
	}

	// get the sql to attach to the database
	attachQuery := fmt.Sprintf(`attach 'ducklake:sqlite:%s' AS %s (
	data_path '%s/', 
	meta_journal_mode 'WAL', 
	meta_synchronous 'NORMAL')`,
		config.GlobalWorkspaceProfile.GetDucklakeDbPath(),
		pconstants.DuckLakeCatalog,
		config.GlobalWorkspaceProfile.GetDataDir())

	commands := []string{
		"-- Install SQLite extension",
		"install sqlite",
		"-- Install DuckLake extension",
		// TODO #DL change to using prod extension when stable
		//  https://github.com/turbot/tailpipe/issues/476
		// _, err = db.Exec("install ducklake;")
		"force install ducklake from core_nightly",
		"-- Attach Tailpipe database",
		attachQuery,
	}

	db, err := database.NewDuckDb(database.WithDuckLakeEnabled(true))
	if err != nil {
		return "", fmt.Errorf("failed to create duckdb: %w", err)
	}
	defer db.Close()

	// get list of tables
	tables, err := database.GetTables(ctx, db)
	if err != nil {
		return "", fmt.Errorf("failed to get db tables: %w", err)
	}

	// get sql to create views for the tables
	viewCommands := getCreateViewsSql(tables, filters...)
	commands = append(commands, viewCommands...)

	// write out the init file
	err = os.WriteFile(initFilePath, []byte(strings.Join(commands, ";\n")+";\n"), 0644)
	if err != nil {
		return "", fmt.Errorf("failed to write init file: %w", err)
	}
	return initFilePath, err
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

func getCreateViewsSql(tables []string, filters ...string) []string {
	// Step 3: Build the where clause
	filterString := ""
	if len(filters) > 0 {
		filterString = fmt.Sprintf(" where %s", strings.Join(filters, " and "))
	}

	results := make([]string, len(tables)*2) // comments + queries
	for i, table := range tables {
		results[i*2] = fmt.Sprintf("-- Query table: %s", table)
		results[i*2+1] = fmt.Sprintf("select * from %s.%s%s", pconstants.DuckLakeCatalog, table, filterString)
	}
	return results
}

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
		fromDate := t.Format(time.DateOnly)
		fromTimestamp := t.Format(time.DateTime)
		result = append(result, fmt.Sprintf("tp_date >= date '%s' and tp_timestamp >= timestamp '%s'", fromDate, fromTimestamp))
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
		toDate := t.Format(time.DateOnly)
		toTimestamp := t.Format(time.DateTime)
		result = append(result, fmt.Sprintf("tp_date <= date '%s' and tp_timestamp <= timestamp '%s'", toDate, toTimestamp))
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

func getPartitionSqlFilters(partitionArgs []string, availablePartitions []string) (string, error) {
	// Get table and partition patterns using getPartitionPatterns
	patterns, err := getPartitionPatterns(partitionArgs, availablePartitions)
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
func replaceWildcards(patterns []parquet.PartitionPattern) []parquet.PartitionPattern {
	updatedPatterns := make([]parquet.PartitionPattern, len(patterns))

	for i, p := range patterns {
		updatedPatterns[i] = parquet.PartitionPattern{
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
