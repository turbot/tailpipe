package filepaths

import (
	"fmt"

	"path/filepath"
)

func GetParquetFileGlobForTable(dataDir, tableName, fileRoot string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/*/*/*/%s*.parquet", tableName, fileRoot))
}

func GetParquetFileGlobForPartition(dataDir, tableName, partitionName, fileRoot string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/tp_partition=%s/*/*/%s*.parquet", tableName, partitionName, fileRoot))
}

func GetParquetPartitionPath(dataDir, tableName, partitionName string) string {
	return filepath.Join(dataDir, fmt.Sprintf("tp_table=%s/tp_partition=%s", tableName, partitionName))
}
