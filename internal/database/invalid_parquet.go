package database

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type InvalidParquet struct {
	InvalidFiles []string `json:"invalid_files"`
}

func LoadInvalidParquet(path string) (*InvalidParquet, error) {
	invalidFiles := &InvalidParquet{}

	// if the file exists, load the invalid parquet file
	if _, err := os.Stat(path); err == nil {
		// load the file and unmarshall into invalidFiles
		data, err := os.ReadFile(path)
		if err != nil {
			slog.Warn("Failed to read invalid parquet file", "file", path, "error", err)
			// just delete the file - it will be regenerated
			if removeErr := os.Remove(path); removeErr != nil {
				// if we cannot remove the existing file, it;s likely we will not be able to replace it so return the error
				return nil, fmt.Errorf("failed to delete unreadable invalid parquet file: %w", removeErr)
			}
		}

		// Unmarshal the JSON content into the InvalidParquet struct
		if err := json.Unmarshal(data, invalidFiles); err != nil {
			slog.Warn("Failed to unmarshal invalid parquet file", "file", path, "error", err)
			// just delete the file - it will be regenerated
			if removeErr := os.Remove(path); removeErr != nil {
				// if we cannot remove the existing file, it;s likely we will not be able to replace it so return the error
				return nil, fmt.Errorf("failed to delete unreadable invalid parquet file: %w", removeErr)
			}
		}
	}

	return invalidFiles, nil
}

func (i *InvalidParquet) Save(path string) error {
	// Marshal the InvalidParquet struct into JSON
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}

	// Write the JSON content to the invalid parquet file
	if err := os.WriteFile(path, data, 0644); err != nil {
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
