package parquet

// partitionKey is used to uniquely identify a a combination of ducklake partition columns:
// tp_table, tp_partition, tp_index, year(tp_timestamp), month(tp_timestamp)
// It also stores the file count for that partition key
type partitionKey struct {
	tpTable     string
	tpPartition string
	tpIndex     string
	year        string // year(tp_timestamp) from partition value
	month       string // month(tp_timestamp) from partition value
	fileCount   int
}

// return the table, escaped for use in a SQL where clause
func (pk partitionKey) safeTable() string {
	return EscapeLiteral(pk.tpTable)
}

// return the partition, escaped for use in a SQL where clause
func (pk partitionKey) safePartition() string {
	return EscapeLiteral(pk.tpPartition)
}

// return the index, escaped for use in a SQL where clause
func (pk partitionKey) safeIndex() string {
	return EscapeLiteral(pk.tpIndex)
}
