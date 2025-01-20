package collector

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type status struct {
	LatestPath               string
	ArtifactsDiscovered      int64
	ArtifactsDownloaded      int64
	ArtifactsDownloadedBytes uint64
	ArtifactsExtracted       int64
	ArtifactErrors           int64
	RowsReceived             int64
	RowsEnriched             int64
	RowsConverted            int64
	Errors                   int64
}

// UpdateWithPluginStatus updates the status with the values from the plugin status event
func (s *status) UpdateWithPluginStatus(event *proto.EventStatus) {
	s.ArtifactsDiscovered = event.ArtifactsDiscovered
	s.ArtifactsDownloaded = event.ArtifactsDownloaded
	s.ArtifactsExtracted = event.ArtifactsExtracted
	s.RowsEnriched = event.RowsEnriched
	s.Errors = int64(event.Errors)
}

func (s *status) SetRowsConverted(rowsConverted int64) {
	s.RowsConverted = rowsConverted
}

func (s *status) String() string {
	return fmt.Sprintf("Artifacts discovered: %d. Artifacts downloaded: %d. Artifacts extracted: %d. Rows enriched: %d. Rows converted: %d. Errors: %d.", s.ArtifactsDiscovered, s.ArtifactsDownloaded, s.ArtifactsExtracted, s.RowsEnriched, s.RowsConverted, s.Errors)
}
