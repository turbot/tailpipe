package parquet

// fileJob represents a single file that needs processed by a worker
type fileJob struct {
	// what group this job belongs to
	groupId string
	// the file chunk number
	chunkNumber int
	// collection type
	collectionType string
	// pointer to the completion count
	completionCount *int32
}
