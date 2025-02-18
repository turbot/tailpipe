package collector

import (
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"

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
	var out strings.Builder
	out.WriteString(fmt.Sprintf("Artifacts discovered: %s. ", humanize.Comma(s.ArtifactsDiscovered)))
	out.WriteString(fmt.Sprintf("Artifacts downloaded: %s. ", humanize.Comma(s.ArtifactsDownloaded)))
	out.WriteString(fmt.Sprintf("Artifacts extracted: %s. ", humanize.Comma(s.ArtifactsExtracted)))
	if s.ArtifactErrors > 0 {
		out.WriteString(fmt.Sprintf("Artifact Errors: %s. ", humanize.Comma(s.ArtifactErrors)))
	}

	out.WriteString(fmt.Sprintf("Rows enriched: %s. ", humanize.Comma(s.RowsEnriched)))
	out.WriteString(fmt.Sprintf("Rows saved: %s. ", humanize.Comma(s.RowsConverted)))
	filteredRows := s.RowsReceived - (s.RowsConverted + s.Errors)
	if filteredRows > 0 {
		out.WriteString(fmt.Sprintf("Rows filtered: %s. ", humanize.Comma(filteredRows)))
	}
	if s.Errors > 0 {
		out.WriteString(fmt.Sprintf("Row Errors: %s.", humanize.Comma(s.Errors)))
	}

	out.WriteString("\n")

	return out.String()
}
