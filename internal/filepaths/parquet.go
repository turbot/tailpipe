package filepaths

import (
	"fmt"
	"path/filepath"

	pfilepaths "github.com/turbot/pipe-fittings/v2/filepaths"
)

const TempParquetExtension = ".parquet.tmp"

func GetParquetFileGlobForTable(dataDir, tableName, fileRoot string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/*/*/*/%s*.parquet", tableName, fileRoot))
}

func GetParquetFileGlobForPartition(dataDir, tableName, partitionName, fileRoot string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/tp_partition=%s/*/*/%s*.parquet", tableName, partitionName, fileRoot))
}

func GetTempParquetFileGlobForPartition(dataDir, tableName, partitionName, fileRoot string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/tp_partition=%s/*/*/%s*%s", tableName, partitionName, fileRoot, TempParquetExtension))
}

func GetParquetPartitionPath(dataDir, tableName, partitionName string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/tp_partition=%s", tableName, partitionName))
}

func InvalidParquetFilePath() string {
	return filepath.Join(pfilepaths.EnsureInternalDir(), "invalid_parquet.json")
}
