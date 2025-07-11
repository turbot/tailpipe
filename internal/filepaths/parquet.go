package filepaths

import (
	"fmt"
	"path/filepath"
)

func GetParquetGlob(basePath, tpTable, tpPartition, executionId string) string {
	// fileRoot is like: data_20250709173630_461829
	// We'll match anything that starts with that
	pattern := fmt.Sprintf(
		"tp_table=%s/tp_partition=%s/tp_index=*/tp_date=*/data_%s*.parquet",
		tpTable,
		tpPartition,
		executionId,
	)

	return filepath.Join(basePath, pattern)
}
