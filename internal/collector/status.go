package collector

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe/internal/parquet"
)

const uiErrorsToDisplay = 3

type status struct {
	events.Status

	rowsConverted         int64
	rowConversionFailures int64
	conversionErrors      []error

	// additional fields for display (not from updates)
	started          time.Time
	complete         bool
	partitionName    string
	fromTime         *row_source.ResolvedFromTime
	compactionStatus *parquet.CompactionStatus
	errorFilePath    string
}

func (s *status) Init(partitionName string, fromTime *row_source.ResolvedFromTime, errorFilePath string) {
	s.started = time.Now()
	s.partitionName = partitionName
	s.fromTime = fromTime
	s.errorFilePath = errorFilePath
}

// UpdateWithPluginStatus updates the status with the values from the plugin status event
func (s *status) UpdateWithPluginStatus(event *proto.EventStatus) {
	s.Status = *events.StatusFromProto(event)
}

func (s *status) UpdateConversionStatus(rowsConverted, failedRows int64, errors ...error) {
	s.rowsConverted = rowsConverted
	s.rowConversionFailures = failedRows
	s.conversionErrors = append(s.conversionErrors, errors...)
}

func (s *status) UpdateCompactionStatus(compactionStatus *parquet.CompactionStatus) {
	if compactionStatus == nil {
		return
	}

	if s.compactionStatus == nil {
		s.compactionStatus = &parquet.CompactionStatus{}
	}

	s.compactionStatus.Update(*compactionStatus)
}

func (s *status) CollectionHeader() string {
	// wrap the source in parentheses if it exists
	fromTimeSource := s.fromTime.Source
	if s.fromTime.Source != "" {
		fromTimeSource = fmt.Sprintf("(%s)", s.fromTime.Source)
	}

	return fmt.Sprintf("\nCollecting logs for %s from %s %s\n\n", s.partitionName, s.fromTime.Time.Format(time.DateOnly), fromTimeSource)
}

func (s *status) String() string {
	var out strings.Builder

	// length of longest keys in the sections (used for spacing/alignment)
	artifactMaxKeyLen := 11
	rowMaxKeyLen := 9

	// establish maximum value lengths for display alignment
	artifactCountLen := len(humanize.Comma(s.ArtifactsDiscovered))
	rowCountLen := len(humanize.Comma(s.RowsReceived))

	// default values
	displayPath := s.LatestArtifactLocation
	timeLabel := "Time:"
	compaction := "Verifying..."

	// override values if complete
	if s.complete {
		displayPath = ""
		timeLabel = "Completed:"
		compaction = "No files to compact."
	}

	// if we have artifacts, we should display the Artifacts section
	if displayPath != "" || s.ArtifactsDiscovered > 0 {
		// obtain file name from path
		if strings.Contains(displayPath, string(os.PathSeparator)) {
			displayPath = displayPath[strings.LastIndex(displayPath, string(os.PathSeparator))+1:]
		}

		// determine what to display for the size of files obtained, default to empty string
		downloadedDisplay := ""
		if s.ArtifactsDownloadedBytes > 0 {
			downloadedDisplay = strings.ReplaceAll(humanize.Bytes(uint64(s.ArtifactsDownloadedBytes)), " ", "")
		}

		// Artifacts section
		out.WriteString("Artifacts:\n")
		out.WriteString(writeCountLine("Discovered:", artifactMaxKeyLen, s.ArtifactsDiscovered, artifactCountLen, &displayPath))
		out.WriteString(writeCountLine("Downloaded:", artifactMaxKeyLen, s.ArtifactsDownloaded, artifactCountLen, &downloadedDisplay))
		out.WriteString(writeCountLine("Extracted:", artifactMaxKeyLen, s.ArtifactsExtracted, artifactCountLen, nil))
		// combine source errors which came form event and from status
		if sourceErrorCount := int64(len(s.SourceErrors)); sourceErrorCount > 0 {
			out.WriteString(writeCountLine("Errors:", artifactMaxKeyLen, sourceErrorCount, artifactCountLen, nil))
		}
		out.WriteString("\n")
	}

	// Rows section
	var rowErrorsTotal int64
	if s.RowErrors != nil {
		rowErrorsTotal = s.RowErrors.Total
	}
	out.WriteString("Rows:\n")
	out.WriteString(writeCountLine("Received:", rowMaxKeyLen, s.RowsReceived, rowCountLen, nil))
	out.WriteString(writeCountLine("Enriched:", rowMaxKeyLen, s.RowsEnriched, rowCountLen, nil))
	out.WriteString(writeCountLine("Saved:", rowMaxKeyLen, s.rowsConverted, rowCountLen, nil))
	rowErrCount := rowErrorsTotal + s.rowConversionFailures
	if rowErrCount > 0 {
		out.WriteString(writeCountLine("Errors:", rowMaxKeyLen, rowErrCount, rowCountLen, nil))
	}
	if s.compactionStatus != nil {
		// TODO: Validate this logic
		filteredRowCount := s.RowsEnriched - (s.rowsConverted + rowErrCount)
		if filteredRowCount > 0 {
			out.WriteString(writeCountLine("Filtered:", rowMaxKeyLen, filteredRowCount, rowCountLen, nil))
		}
	}
	out.WriteString("\n")

	// Files section
	if s.compactionStatus != nil {
		out.WriteString("Files:\n")
		if s.compactionStatus.Source == 0 && s.compactionStatus.Uncompacted == 0 {
			out.WriteString(fmt.Sprintf("  %s\n", compaction))
		} else {
			l := int64(s.compactionStatus.Source + s.compactionStatus.Uncompacted)
			r := int64(s.compactionStatus.Dest + s.compactionStatus.Uncompacted)
			out.WriteString(fmt.Sprintf("  Compacted: %s => %s\n", humanize.Comma(l), humanize.Comma(r)))
		}
		out.WriteString("\n")
	}

	// Errors section
	if rowErrorsTotal > 0 {
		out.WriteString("Errors:\n")
		rowErrors := s.RowErrors.Errors()
		for i, e := range rowErrors {
			if i <= (uiErrorsToDisplay - 1) {
				out.WriteString(fmt.Sprintf("  %s\n", e))
			} else {
				out.WriteString(fmt.Sprintf("  … and %d more.\n", len(rowErrors)-uiErrorsToDisplay))
				out.WriteString(fmt.Sprintf("  See %s for full details.\n", s.errorFilePath))
				break
			}
		}
		out.WriteString("\n")
	}

	// Timing Section
	duration := time.Since(s.started)
	out.WriteString(fmt.Sprintf("%s %s\n", timeLabel, utils.HumanizeDuration(duration)))

	// return View
	return out.String()
}

func writeCountLine(desc string, maxDescLen int, count int64, maxCountLen int, suffix *string) string {
	s := ""
	if suffix != nil {
		s = fmt.Sprintf(" %s", *suffix)
	}
	return fmt.Sprintf("  %-*s %*s%s\n", maxDescLen, desc, maxCountLen, humanize.Comma(count), s)
}
