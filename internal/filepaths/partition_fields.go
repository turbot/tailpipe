package filepaths

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PartitionFields represents the components of a parquet file path
type PartitionFields struct {
	Table     string
	Partition string
	Date      time.Time
	Index     int
}

// ExtractPartitionFields parses a parquet file path and returns its components.
// Expected path format:
//
//	/path/to/dir/tp_table=<table_name>/tp_partition=<partition_id>/tp_date=<YYYY-MM-DD>/tp_index=<index>/file.parquet
//
// Rules:
//   - Fields can appear in any order
//   - It is an error for the same field to appear with different values
//   - Date must be in YYYY-MM-DD format
//   - Missing fields are allowed (will have zero values)
func ExtractPartitionFields(parquetFilePath string) (PartitionFields, error) {
	fields := PartitionFields{}

	parts := strings.Split(parquetFilePath, "/")
	for _, part := range parts {
		switch {
		case strings.HasPrefix(part, "tp_table="):
			value := strings.TrimPrefix(part, "tp_table=")
			if fields.Table != "" && fields.Table != value {
				return PartitionFields{}, fmt.Errorf("conflicting table values: %s and %s", fields.Table, value)
			}
			fields.Table = value
		case strings.HasPrefix(part, "tp_partition="):
			value := strings.TrimPrefix(part, "tp_partition=")
			if fields.Partition != "" && fields.Partition != value {
				return PartitionFields{}, fmt.Errorf("conflicting partition values: %s and %s", fields.Partition, value)
			}
			fields.Partition = value
		case strings.HasPrefix(part, "tp_date="):
			value := strings.TrimPrefix(part, "tp_date=")
			date, err := time.Parse("2006-01-02", value)
			if err == nil {
				if !fields.Date.IsZero() && !fields.Date.Equal(date) {
					return PartitionFields{}, fmt.Errorf("conflicting date values: %s and %s", fields.Date.Format("2006-01-02"), value)
				}
				fields.Date = date
			}
		case strings.HasPrefix(part, "tp_index="):
			value := strings.TrimPrefix(part, "tp_index=")
			if fields.Index != 0 {
				if index, err := strconv.Atoi(value); err == nil {
					if fields.Index != index {
						return PartitionFields{}, fmt.Errorf("conflicting index values: %d and %s", fields.Index, value)
					}
				}
			} else {
				if index, err := strconv.Atoi(value); err == nil {
					fields.Index = index
				}
			}
		}
	}

	return fields, nil
}
