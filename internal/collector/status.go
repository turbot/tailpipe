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
	PluginErrors             int64
	ConversionErrors         int64
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
	s.PluginErrors = event.Errors
}

func (s *status) UpdateConversionStatus(rowsConverted, errors int64) {
	s.RowsConverted = rowsConverted
	s.ConversionErrors = errors
}

func (s *status) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("Artifacts discovered: %s. ", humanize.Comma(s.ArtifactsDiscovered)))
	out.WriteString(fmt.Sprintf("Artifacts downloaded: %s. ", humanize.Comma(s.ArtifactsDownloaded)))
	out.WriteString(fmt.Sprintf("Artifacts extracted: %s. ", humanize.Comma(s.ArtifactsExtracted)))
	if s.ArtifactErrors > 0 {
		out.WriteString(fmt.Sprintf("Artifact PluginErrors: %s. ", humanize.Comma(s.ArtifactErrors)))
	}

	out.WriteString(fmt.Sprintf("Rows enriched: %s. ", humanize.Comma(s.RowsEnriched)))
	out.WriteString(fmt.Sprintf("Rows saved: %s. ", humanize.Comma(s.RowsConverted)))
	filteredRows := s.RowsReceived - (s.RowsConverted + s.PluginErrors + s.ConversionErrors)
	if filteredRows > 0 {
		out.WriteString(fmt.Sprintf("Rows filtered: %s. ", humanize.Comma(filteredRows)))
	}
	if s.PluginErrors > 0 {
		out.WriteString(fmt.Sprintf("Plugin errors: %s.", humanize.Comma(s.PluginErrors)))
	}
	if s.ConversionErrors > 0 {
		out.WriteString(fmt.Sprintf("Conversion errors: %s.", humanize.Comma(s.PluginErrors)))
	}

	out.WriteString("\n")

	return out.String()
}
