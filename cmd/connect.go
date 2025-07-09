package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/exp/maps"
	"io"
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
		Short: "Return a connection string for a database, with a schema determined by the provided parameters",
		Long:  `Return a connection string for a database, with a schema determined by the provided parameters.`,
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
	var databaseFilePath string
	ctx := cmd.Context()

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		setExitCodeForConnectError(err)
		displayOutput(ctx, databaseFilePath, err)
	}()

	// TODO decide what to return

	// we are done - the defer block will print either the filepath (if successful) or the error (if not)
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

// cleanupOldDbFiles deletes old db files(older than a day) that are not in use
func cleanupOldDbFiles() error {
	baseDir := pfilepaths.GetDataDir()
	log.Printf("[INFO] Cleaning up old db files in %s\n", baseDir)
	cutoffTime := time.Now().Add(-constants.DbFileMaxAge) // Files older than 1 day

	// The baseDir ("$TAILPIPE_INSTALL_DIR/data") is expected to have subdirectories for different workspace
	// profiles(default, work etc). Each subdirectory may contain multiple .db files.
	// Example structure:
	// data/
	// ├── default/
	// │   ├── tailpipe_20250115182129.db
	// │   ├── tailpipe_20250115193816.db
	// │   ├── tailpipe.db
	// │   └── ...
	// ├── work/
	// │   ├── tailpipe_20250115182129.db
	// │   ├── tailpipe_20250115193816.db
	// │   ├── tailpipe.db
	// │   └── ...
	// So we traverse all these subdirectories for each workspace and process the relevant files.
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing path %s: %v", path, err)
		}

		// skip directories and non-`.db` files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".db") {
			return nil
		}

		// skip `tailpipe.db` file
		if info.Name() == "tailpipe.db" {
			return nil
		}

		// only process `tailpipe_*.db` files
		if !strings.HasPrefix(info.Name(), "tailpipe_") {
			return nil
		}

		// check if the file is older than the cutoff time
		if info.ModTime().After(cutoffTime) {
			log.Printf("[DEBUG] Skipping deleting file %s(%s) as it is not older than %s\n", path, info.ModTime().String(), cutoffTime)
			return nil
		}

		// check for a lock on the file
		db, err := database.NewDuckDb(database.WithDbFile(path))
		if err != nil {
			log.Printf("[INFO] Skipping deletion of file %s due to error: %v\n", path, err)
			return nil
		}
		defer db.Close()

		// if no lock, delete the file
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

// getPartitionPatterns returns the table and partition patterns for the given partition args
func getPartitionPatterns(partitionArgs []string, partitions []string) ([]parquet.PartitionPattern, error) {
	var res []parquet.PartitionPattern
	for _, arg := range partitionArgs {
		tablePattern, partitionPattern, err := getPartitionMatchPatternsForArg(partitions, arg)
		if err != nil {
			return nil, fmt.Errorf("error processing partition arg '%s': %w", arg, err)
		}

		res = append(res, parquet.PartitionPattern{Table: tablePattern, Partition: partitionPattern})
	}

	return res, nil
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
