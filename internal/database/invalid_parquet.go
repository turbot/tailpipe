package database

import (
	"encoding/json"
	"fmt"
	"github.com/turbot/tailpipe/internal/filepaths"
	"log/slog"
	"os"
	"regexp"
	"time"
)

// GenerateInvalidParquetMessage builds a user message for an invalid parquet file error
func GenerateInvalidParquetMessage(parquetFilePath string) (string, error) {
	// determine the table and partition name and date from the filepath
	// Define regex pattern to extract tp_table, tp_partition, and tp_date
	re := regexp.MustCompile(`tp_table=([^/]+)/tp_partition=([^/]+)/.*?/tp_date=([^/]+)`)

	var table, partition, dateString string
	// Find matches
	match := re.FindStringSubmatch(parquetFilePath)
	if len(match) <= 3 {
		return "", fmt.Errorf("failed to extract table, partition, and date from parquet file path: %s", parquetFilePath)
	}
	table = match[1]
	partition = match[2]
	dateString = match[3]

	// parse the date string - format 2025-02-21
	_, err := time.Parse("2006-01-02", dateString)
	if err != nil {
		return "", fmt.Errorf("failed to parse date string: %s", dateString)
	}

	partitionName := fmt.Sprintf("%s.%s", table, partition)
	msg := fmt.Sprintf("An invalid parquet file was detected.\nThe invalid file was renamed to '%s'.\n\n** Data collection may be incomplete for partition '%s' **\n\nRecollect using the command:\n\t\ttailpipe collect %s --from %s",
		parquetFilePath, partitionName, partitionName, dateString)

	return msg, nil

}

type InvalidParquet struct {
	InvalidFiles []string `json:"invalid_files"`
}

func LoadInvalidParquet() (*InvalidParquet, error) {
	invalidFiles := &InvalidParquet{}

	// if the file exists, load the invalid parquet file
	invalidParquetFilePath := filepaths.InvalidParquetFilePath()
	if _, err := os.Stat(invalidParquetFilePath); err == nil {
		// load the file and unmarshall into invalidFiles
		data, err := os.ReadFile(invalidParquetFilePath)
		if err != nil {
			slog.Warn("Failed to read invalid parquet file", "file", invalidParquetFilePath, "error", err)
			// just delete the file - it will be regenerated
			if removeErr := os.Remove(invalidParquetFilePath); removeErr != nil {
				// if we cannot remove the existing file, it;s likely we will not be able to replace it so return the error
				return nil, fmt.Errorf("failed to delete unreadable invalid parquet file: %w", removeErr)
			}
		}

		// Unmarshal the JSON content into the InvalidParquet struct
		if err := json.Unmarshal(data, invalidFiles); err != nil {
			slog.Warn("Failed to unmarshal invalid parquet file", "file", invalidParquetFilePath, "error", err)
			// just delete the file - it will be regenerated
			if removeErr := os.Remove(invalidParquetFilePath); removeErr != nil {
				// if we cannot remove the existing file, it;s likely we will not be able to replace it so return the error
				return nil, fmt.Errorf("failed to delete unreadable invalid parquet file: %w", removeErr)
			}
		}
	}

	return invalidFiles, nil
}

func (i *InvalidParquet) Save() error {
	// Marshal the InvalidParquet struct into JSON
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}

	// Write the JSON content to the invalid parquet file
	if err := os.WriteFile(filepaths.InvalidParquetFilePath(), data, 0644); err != nil {
		return err
	}

	return nil
}

// AddFile adds a file to the list of invalid parquet files
func (i *InvalidParquet) AddFile(fileName string) {
	// check for dupes
	for _, f := range i.InvalidFiles {
		if f == fileName {
			return
		}
	}
	i.InvalidFiles = append(i.InvalidFiles, fileName)
}

// RemoveFile removes a file from the list of invalid parquet files
func (i *InvalidParquet) RemoveFile(fileName string) {
	// find the file in the list
	for idx, f := range i.InvalidFiles {
		if f == fileName {
			// remove the file from the list
			i.InvalidFiles = append(i.InvalidFiles[:idx], i.InvalidFiles[idx+1:]...)
			return
		}
	}
}
