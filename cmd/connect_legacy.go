package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/turbot/go-kit/helpers"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/v2/connection"
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/pipe-fittings/v2/error_helpers"
	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
	"github.com/turbot/tailpipe/internal/config"
	"github.com/turbot/tailpipe/internal/constants"
	"github.com/turbot/tailpipe/internal/database"
	"github.com/turbot/tailpipe/internal/filepaths"
)

func runLegacyConnectCmd(cmd *cobra.Command, _ []string) {
	var err error
	var databaseFilePath string
	ctx := cmd.Context()

	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
		setExitCodeForConnectError(err)
		displayOutputLegacy(ctx, databaseFilePath, err)
	}()

	databaseFilePath, err = generateDbFile(ctx)

	// we are done - the defer block will print either the filepath (if successful) or the error (if not)
}

func generateDbFile(ctx context.Context) (string, error) {
	databaseFilePath := generateTempDBFilename(config.GlobalWorkspaceProfile.GetDataDir())

	// cleanup the old db files if not in use
	err := cleanupOldDbFiles()
	if err != nil {
		return "", err
	}

	// first build the filters
	filters, err := getFilters()
	if err != nil {
		return "", fmt.Errorf("error building filters: %w", err)
	}

	// if there are no filters, just copy the db file
	if len(filters) == 0 {
		err = copyDBFile(filepaths.TailpipeLegacyDbFilePath(), databaseFilePath)
		return databaseFilePath, err
	}

	// Open a DuckDB connection (creates the file if it doesn't exist)
	db, err := database.NewDuckDb(database.WithDbFile(databaseFilePath))

	if err != nil {
		return "", fmt.Errorf("failed to open DuckDB connection: %w", err)
	}
	defer db.Close()

	err = database.AddTableViews(ctx, db, filters...)
	return databaseFilePath, err
}

func displayOutputLegacy(ctx context.Context, databaseFilePath string, err error) {
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

// generateTempDBFilename generates a temporary filename with a timestamp
func generateTempDBFilename(dataDir string) string {
	timestamp := time.Now().Format("20060102150405") // e.g., 20241031103000
	return filepath.Join(dataDir, fmt.Sprintf("tailpipe_%s.db", timestamp))
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
	cutoffTime := time.Now().Add(-constants.LegacyDbFileMaxAge) // Files older than 1 day

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
