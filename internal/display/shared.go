package display

import (
	"math"
	"os"
	"path/filepath"

	"github.com/dustin/go-humanize"
)

type FileMetadata struct {
	FileSize  int64 `json:"file_size"`
	FileCount int64 `json:"file_count"`
}

func (f *FileMetadata) HumanizeSize() string {
	return humanizeBytes(f.FileSize)
}

func (f *FileMetadata) HumanizeCount() string {
	return humanize.Comma(f.FileCount)
}

func humanizeBytes(bytes int64) string {
	return humanize.Bytes(uint64(math.Max(float64(bytes), 0)))
}

func getFileMetadata(basePath string) (FileMetadata, error) {
	var metadata FileMetadata

	// if basePath doesn't exist - nothing collected so short-circuit
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return metadata, nil
	}

	// Get File Information
	err := filepath.Walk(basePath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		metadata.FileCount++
		metadata.FileSize += info.Size()

		return nil
	})

	return metadata, err
}
