package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/turbot/go-kit/files"
	"github.com/turbot/pipe-fittings/v2/statushooks"
)

const (
	ROWS_PER_FILE   = 10000 // Number of rows per log file before creating a new file
	FILES_PER_FOLDER = 10  // Number of files per folder before creating a new subfolder
)

var (
	goodRowFormat  = "%s 2 %d eni-0a86803e1af18f146 - - - - - - - 1720072205 1720072282 - NODATA\n"
	errorRowFormat = "2 %d %s eni-0a86803e1af18f146 - - - - - - - 1720072205 1720072282 - NODATA\n"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: test_data <total> <errors> <dest>")
		os.Exit(1)
	}

	spinner := statushooks.NewStatusSpinnerHook(statushooks.WithMessage("Generating logs..."))
	spinner.Show()
	defer spinner.Hide()
	totalRows, err := strconv.Atoi(os.Args[1])
	if err != nil || totalRows <= 0 {
		fmt.Println("Invalid total rows value. Must be an positive integer.")
		os.Exit(1)
	}

	errorRows, err := strconv.Atoi(os.Args[2])
	if err != nil || errorRows < 0 {
		fmt.Println("Invalid error rows value. Must be an integer.")
		os.Exit(1)
	}

	destDir, err := files.Tildefy(os.Args[3])
	if err != nil {
		fmt.Println("Invalid destination directory:", err)
		os.Exit(1)
	}
	// Create the destination directory, deleting if exists
	if err := os.MkdirAll(destDir, 0755); err != nil {
		fmt.Println("Error creating destination directory:", err)
		os.Exit(1)
	}

	// Calculate error interval
	errorInterval, nextErrorRow := 0, 0
	if errorRows > 0 {
		if errorRows >= totalRows {
			// Every row will be an error row
			errorInterval = 1
		} else {
			errorInterval = totalRows / errorRows
		}
		nextErrorRow = errorInterval
	}

	startTime := time.Now()
	fileIndex := 1
	folderIndex := 1
	currentRow := 0
	var file *os.File

	for i := 1; i <= totalRows; i++ {
		if i % 10000 == 0 {
			spinner.SetStatus(fmt.Sprintf("Generating logs... %d/%d", i, totalRows))
		}
		// Create a new folder if needed
		if (fileIndex-1)%FILES_PER_FOLDER == 0 {
			subFolder := filepath.Join(destDir, fmt.Sprintf("folder_%d", folderIndex))
			if err := os.MkdirAll(subFolder, 0755); err != nil {
				fmt.Println("Error creating subdirectory:", err)
				os.Exit(1)
			}
			folderIndex++
		}

		// Open a new file if needed
		if currentRow%ROWS_PER_FILE == 0 {
			if file != nil {
				if err := file.Close(); err != nil {
					fmt.Println("Error closing file:", err)
				}
			}
			fileName := filepath.Join(destDir, fmt.Sprintf("folder_%d", folderIndex-1), fmt.Sprintf("logfile_%d.log", fileIndex))
			file, err = os.Create(fileName)
			if err != nil {
				fmt.Println("Error creating file:", err)
				os.Exit(1)
			}
			fileIndex++
		}

		// Generate log entry
		var logEntry string
		if errorInterval > 0 && i == nextErrorRow {
			logEntry = fmt.Sprintf(errorRowFormat, i, "ERROR")
			nextErrorRow += errorInterval
		} else {
			timestamp := startTime.Add(time.Duration(i) * time.Millisecond).UTC().Format("2006-01-02T15:04:05.000Z")
			logEntry = fmt.Sprintf(goodRowFormat, timestamp, i)
		}

		// Write to file
		if _, err := file.WriteString(logEntry); err != nil {
			fmt.Println("Error writing to file:", err)
			os.Exit(1)
		}

		currentRow++
	}

	// Close the last file
	if file != nil {
		if err := file.Close(); err != nil {
			fmt.Println("Error closing file:", err)
			os.Exit(1)
		}
	}

	fmt.Println("Log generation complete. Files are in:", destDir)
}
