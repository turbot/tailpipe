package collector

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/table"
)

type status struct {
	ArtifactsDiscovered int64
	ArtifactsDownloaded int64
	ArtifactsExtracted  int64
	RowsEnriched        int64
	RowsConverted       int64
	Errors              int32
}

// UpdateWithPluginStatus updates the status with the values from the plugin status event
func (s *status) UpdateWithPluginStatus(event *proto.EventStatus) {
	s.ArtifactsDiscovered = event.ArtifactsDiscovered
	s.ArtifactsDownloaded = event.ArtifactsDownloaded
	s.ArtifactsExtracted = event.ArtifactsExtracted
	s.RowsEnriched = event.RowsEnriched
	s.Errors = event.Errors
}

func (s *status) SetRowsConverted(rowsConverted int64) {
	s.RowsConverted = rowsConverted
}

func (s *status) setChunksWritten(chunksWritten int32) {
	// TODO K this assumes a fixed chunk size which will not be the case for direct artefact-json conversion tables
	// https://github.com/turbot/tailpipe/issues/75
	// convert to rows written (clamp at rows enriched to allow for partial final chunk)
	s.RowsConverted = min(int64(chunksWritten)*table.JSONLChunkSize, s.RowsEnriched)
}

func (s *status) String() string {
	return fmt.Sprintf("Artifacts discovered: %d. Artifacts downloaded: %d. Artifacts extracted: %d. Rows enriched: %d. Rows converted: %d. Errors: %d.", s.ArtifactsDiscovered, s.ArtifactsDownloaded, s.ArtifactsExtracted, s.RowsEnriched, s.RowsConverted, s.Errors)
}
