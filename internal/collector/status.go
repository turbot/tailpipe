package collector

import (
	"fmt"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
)

type status struct {
	LatestArtifactPath       string
	ArtifactsDiscovered      int64
	ArtifactsDownloaded      int64
	ArtifactsDownloadedBytes int64
	ArtifactsExtracted       int64
	ArtifactErrors           int64
	RowsReceived             int64
	RowsEnriched             int64
	RowsConverted            int64
	Errors                   int64
}

// UpdateWithPluginStatus updates the status with the values from the plugin status event
func (s *status) UpdateWithPluginStatus(event *proto.EventStatus) {
	s.LatestArtifactPath = event.LatestArtifactPath
	s.ArtifactsDiscovered = event.ArtifactsDiscovered
	s.ArtifactsDownloaded = event.ArtifactsDownloaded
	s.ArtifactsDownloadedBytes = event.ArtifactsDownloadedBytes
	s.ArtifactsExtracted = event.ArtifactsExtracted
	s.ArtifactErrors = event.ArtifactErrors
	s.RowsReceived = event.RowsReceived
	s.RowsEnriched = event.RowsEnriched
	s.Errors = event.Errors
}

func (s *status) SetRowsConverted(rowsConverted int64) {
	s.RowsConverted = rowsConverted
}

func (s *status) String() string {
	return fmt.Sprintf("Artifacts discovered: %d. Artifacts downloaded: %d. Artifacts extracted: %d. Rows enriched: %d. Rows converted: %d. Errors: %d.", s.ArtifactsDiscovered, s.ArtifactsDownloaded, s.ArtifactsExtracted, s.RowsEnriched, s.RowsConverted, s.Errors)
}
