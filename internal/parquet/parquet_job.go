package parquet

//
//// parquetJob represents a single file that needs processed by a worker
//type parquetJob struct {
//	// what group this job belongs to
//	groupId string
//	// the file chunk number
//	chunkNumber int
//	// pointer to the completion count
//	completionCount *int32
//	// pointer to the row count
//	rowCount *int64
//
//	Partition  *config.Partition
//	SchemaFunc func() *schema.TableSchema
//}
