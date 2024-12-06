package parquet

//
//type ConversionPool struct {
//	// the number of conversions that can be done in parallel
//}
//
//
//func NewConversionPool(sourceDir, destDir string, workers int) (*ConversionPool, error) {
//	w := &ConversionPool{
//		//jobPool: newFileJobPool(workers, sourceDir, destDir, newParquetConversionWorker),
//	}
//
//	if err := w.Start(); err != nil {
//		w.Close()
//		return nil, fmt.Errorf("failed to start parquet ConversionPool: %w", err)
//	}
//	return w, nil
//}
//
//// Start the parquet ConversionPool - spawn workers
//func (w *ConversionPool) Start() error {
//	return w.jobPool.Start()
//
//}
//
//// StartJobGroup schedules a jobGroup to be processed
//// it adds an entry to the jobGroups map and starts a goroutine to schedule the jobGroup
//func (w *ConversionPool) StartJobGroup(executionId string, payload JobPayload) error {
//	slog.Info("parquet.ConversionPool.StartJobGroup", "jobGroupId", executionId, "partitionName", payload.Partition.UnqualifiedName)
//
//	return w.jobPool.StartJobGroup(executionId, payload)
//
//}
//
//// AddJob adds available jobs to a jobGroup
//// this is making the assumption that all files for a jobGroup are have a filename of format <execution_id>_<chunkNumber>.jsonl
//// therefore we only need to pass the chunkNumber number
//func (w *ConversionPool) AddJob(executionID string, chunks ...int) error {
//	return w.jobPool.AddChunk(executionID, chunks...)
//
//}
//
//func (w *ConversionPool) GetChunksWritten(id string) (int32, error) {
//	return w.jobPool.GetChunksWritten(id)
//}
//
//func (w *ConversionPool) JobGroupComplete(executionId string) error {
//	return w.jobPool.JobGroupComplete(executionId)
//}
//
//func (w *ConversionPool) Close() {
//	w.jobPool.Close()
//}
