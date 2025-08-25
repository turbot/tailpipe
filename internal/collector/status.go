package collector

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/logging"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe/internal/parquet"
)

const uiErrorsToDisplay = 15

type status struct {
	events.Status

	rowsConverted         int64
	rowConversionFailures int64
	conversionErrors      []string

	// additional fields for display (not from updates)
	started          time.Time
	complete         bool
	partitionName    string
	fromTime         *row_source.ResolvedFromTime
	compactionStatus *parquet.CompactionStatus
	toTime           time.Time
}

// Init initializes the status with the partition name and  resolved from time of the collection and marks start of collection for timing
func (s *status) Init(partitionName string, fromTime *row_source.ResolvedFromTime, toTime time.Time) {
	s.started = time.Now()
	s.partitionName = partitionName
	s.fromTime = fromTime
	s.toTime = toTime
}

// UpdateWithPluginStatus updates the status with the values from the plugin status event
func (s *status) UpdateWithPluginStatus(event *events.Status) {
	s.Status = *event
}

// UpdateConversionStatus updates the status with rows converted, rows the conversion failed on, and any errors
func (s *status) UpdateConversionStatus(rowsConverted, failedRows int64, errors ...error) {
	s.rowsConverted = rowsConverted
	s.rowConversionFailures = failedRows
	if len(errors) > 0 {
		for _, err := range errors {
			s.conversionErrors = append(s.conversionErrors, err.Error())
		}
	}
}

// UpdateCompactionStatus updates the status with the values from the compaction status event
func (s *status) UpdateCompactionStatus(compactionStatus *parquet.CompactionStatus) {
	if compactionStatus == nil {
		return
	}

	if s.compactionStatus == nil {
		s.compactionStatus = compactionStatus
		return
	}

	s.compactionStatus.Update(*compactionStatus)
}

// CollectionHeader returns a string to display at the top of the collection status for app or alone for non-progress display
func (s *status) CollectionHeader() string {
	// wrap the source in parentheses if it exists
	fromTimeSource := s.fromTime.Source
	if s.fromTime.Source != "" {
		fromTimeSource = fmt.Sprintf(" (%s)", s.fromTime.Source)
	}

	return fmt.Sprintf("\nCollecting logs for %s from %s%s to %s\n\n", s.partitionName, s.fromTime.Time.Format(time.DateOnly), fromTimeSource, s.toTime.Format(time.DateOnly))
}

// String returns a string representation of the status used as body of app display or final output for non-progress display
func (s *status) String() string {
	var out strings.Builder

	// determine if we should show an Artifacts or Source section (source currently only shown if we have errors)
	switch {
	case s.ArtifactsDiscovered > 0 || s.LatestArtifactLocation != "":
		out.WriteString(s.displayArtifactSection())
	case len(s.SourceErrors) > 0:
		out.WriteString(s.displaySourceSection())
	}

	// Rows section
	out.WriteString(s.displayRowSection())

	// Files section
	out.WriteString(s.displayFilesSection())

	// Errors section
	out.WriteString(s.displayErrorsSection())

	// Timing Section
	out.WriteString(s.displayTimingSection())

	// return View
	return out.String()
}

// displayArtifactSection returns a string representation of the artifact section of the status, used for artifact collections
func (s *status) displayArtifactSection() string {
	// length of longest key in the section (used for spacing/alignment)
	artifactMaxKeyLen := 11

	// establish maximum value lengths for display alignment
	artifactCountLen := len(humanize.Comma(s.ArtifactsDiscovered))

	// obtain file name from artifact path if we're not completed (we don't show the path if we are)
	displayPath := ""
	if !s.complete && s.LatestArtifactLocation != "" {
		displayPath = filepath.Base(s.LatestArtifactLocation)
	}

	// determine file size to display human friendly
	displaySize := ""
	if s.ArtifactsDownloadedBytes > 0 {
		displaySize = strings.ReplaceAll(humanize.Bytes(uint64(s.ArtifactsDownloadedBytes)), " ", "") //nolint:gosec // should be safe
	}

	// obtain error count
	sourceErrorCount := int64(len(s.SourceErrors))

	// build artifact section
	var out strings.Builder
	out.WriteString("Artifacts:\n")
	out.WriteString(writeCountLine("Discovered:", artifactMaxKeyLen, s.ArtifactsDiscovered, artifactCountLen, &displayPath))
	out.WriteString(writeCountLine("Downloaded:", artifactMaxKeyLen, s.ArtifactsDownloaded, artifactCountLen, &displaySize))
	out.WriteString(writeCountLine("Extracted:", artifactMaxKeyLen, s.ArtifactsExtracted, artifactCountLen, nil))
	if sourceErrorCount > 0 {
		out.WriteString(writeCountLine("Errors:", artifactMaxKeyLen, sourceErrorCount, artifactCountLen, nil))
	}
	out.WriteString("\n")

	return out.String()
}

// displaySourceSection returns a string representation of the source section of the status, used for non-artifact collections
func (s *status) displaySourceSection() string {
	// length of longest key in the section (used for spacing/alignment)
	sourceMaxKeyLen := 7

	// obtain error count
	sourceErrorCount := int64(len(s.SourceErrors))

	// if no errors, nothing to display
	if sourceErrorCount == 0 {
		return ""
	}

	// build source section
	var out strings.Builder
	out.WriteString("Source:\n")
	out.WriteString(writeCountLine("Errors:", sourceMaxKeyLen, sourceErrorCount, len(humanize.Comma(sourceErrorCount)), nil))
	out.WriteString("\n")

	return out.String()
}

// displayRowSection returns a string representation of the row section of the status (rows received, enriched, saved, errors, and filtered)
func (s *status) displayRowSection() string {
	// length of longest key in the section (used for spacing/alignment)
	rowMaxKeyLen := 9

	// establish maximum value lengths for display alignment
	rowCountLen := len(humanize.Comma(s.RowsReceived))

	// obtain error count
	var rowErrorCount int64
	if s.RowErrors != nil {
		rowErrorCount = s.RowErrors.Total
	}
	rowErrorCount += s.rowConversionFailures

	// determine if we show a filtered count (we should be in compaction)
	var filteredRowCount int64
	if s.compactionStatus != nil {
		filteredRowCount = s.RowsEnriched - (s.rowsConverted + rowErrorCount)
	}

	// build row section
	var out strings.Builder
	out.WriteString("Rows:\n")
	out.WriteString(writeCountLine("Received:", rowMaxKeyLen, s.RowsReceived, rowCountLen, nil))
	out.WriteString(writeCountLine("Enriched:", rowMaxKeyLen, s.RowsEnriched, rowCountLen, nil))
	out.WriteString(writeCountLine("Saved:", rowMaxKeyLen, s.rowsConverted, rowCountLen, nil))
	if rowErrorCount > 0 {
		out.WriteString(writeCountLine("Errors:", rowMaxKeyLen, rowErrorCount, rowCountLen, nil))
	}
	if filteredRowCount > 0 {
		out.WriteString(writeCountLine("Filtered:", rowMaxKeyLen, filteredRowCount, rowCountLen, nil))
	}
	out.WriteString("\n")

	return out.String()
}

// displayFilesSection returns a string representation of the files section of the status (compacted file counts, etc.)
func (s *status) displayFilesSection() string {
	// if we're not at compaction, don't display this section
	if s.compactionStatus == nil {
		return ""
	}

	// determine status text to use when no counts are available
	statusText := "Verifying..."
	if s.complete {
		statusText = "No files to compact."
	}

	var out strings.Builder
	out.WriteString("Files:\n")
	if s.compactionStatus.Source == 0 && s.compactionStatus.Uncompacted == 0 {
		// no counts available, display status text
		out.WriteString(fmt.Sprintf("  %s\n", statusText))
	} else {
		// display counts source => dest
		l := int64(s.compactionStatus.Source + s.compactionStatus.Uncompacted)
		r := int64(s.compactionStatus.Dest + s.compactionStatus.Uncompacted)
		out.WriteString(fmt.Sprintf("  Compacted: %s => %s\n", humanize.Comma(l), humanize.Comma(r)))
	}

	out.WriteString("\n")

	return out.String()
}

// displayErrorsSection returns a string representation of the errors section of the status (limited to uiErrorsToDisplay, with help on how to see more details)
func (s *status) displayErrorsSection() string {
	// get error counts - if all 0 we don't display this section
	var rowErrorsCount, convErrorsCount, srcErrorCount int
	var rowErrors []string
	if s.RowErrors != nil {
		rowErrors = s.RowErrors.Errors()
		rowErrorsCount = len(rowErrors)
	}
	convErrorsCount = len(s.conversionErrors)
	srcErrorCount = len(s.SourceErrors)
	totalErrors := rowErrorsCount + convErrorsCount + srcErrorCount

	// if we have no errors, nothing to display
	if totalErrors == 0 {
		return ""
	}

	// make a fixed size slice to hold the errors we'll display
	displayErrors := make([]string, 0, uiErrorsToDisplay)

	// determine max errors to display (if we have less than our uiErrorsToDisplay, we'll display all)
	displaySrc, displayConv, displayRow := errorCountsToDisplay(srcErrorCount, convErrorsCount, rowErrorsCount, uiErrorsToDisplay)
	displayErrors = append(displayErrors, s.SourceErrors[:displaySrc]...)
	displayErrors = append(displayErrors, s.conversionErrors[:displayConv]...)
	displayErrors = append(displayErrors, rowErrors[:displayRow]...)

	// build errors section
	var out strings.Builder
	out.WriteString("Errors:\n")

	// display the errors
	for _, e := range displayErrors {
		out.WriteString(fmt.Sprintf("  %s\n", e))
	}

	// if we have more than shown, display truncated message
	truncatedString := ""
	if totalErrors > uiErrorsToDisplay {
		truncatedString = "Truncated. "
	}
	// inform user how to see more details via setting log level
	out.WriteString(fmt.Sprintf("  %sSet %s=ERROR for details.\n", truncatedString, logging.EnvLogLevel))

	out.WriteString("\n")

	return out.String()
}

// displayTimingSection returns a string representation of the timing section of the status (time elapsed since start of collection)
func (s *status) displayTimingSection() string {
	duration := time.Since(s.started)

	// if we're complete, change the time label to show this
	if s.complete {
		if s.compactionStatus != nil && s.compactionStatus.Duration > 0 {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("Collection: %s\n", utils.HumanizeDuration(duration)))
			sb.WriteString(fmt.Sprintf("Compaction: %s\n", utils.HumanizeDuration(s.compactionStatus.Duration)))
			sb.WriteString(fmt.Sprintf("Total: %s\n", utils.HumanizeDuration(duration+s.compactionStatus.Duration)))
			return sb.String()
		}
		return fmt.Sprintf("Completed: %s\n", utils.HumanizeDuration(duration))
	} else {
		// if not complete, show elapsed time
		return fmt.Sprintf("Time: %s\n", utils.HumanizeDuration(duration))
	}

}

// writeCountLine returns a formatted string for a count line in the status display, used for alignment and readability
func writeCountLine(desc string, maxDescLen int, count int64, maxCountLen int, suffix *string) string {
	s := ""
	if suffix != nil {
		s = fmt.Sprintf(" %s", *suffix)
	}
	return fmt.Sprintf("  %-*s %*s%s\n", maxDescLen, desc, maxCountLen, humanize.Comma(count), s)
}

// errorCountsToDisplay determines how many of each type of error to display in the status, based on the counts and a maximum limit
func errorCountsToDisplay(srcCount, convCount, rowCount, max int) (displaySrc, displayConv, displayRow int) {
	// initial allotment of display slots, capped at 1/3 of max
	baseLimit := max / 3
	displaySrc = min(srcCount, baseLimit)
	displayConv = min(convCount, baseLimit)
	displayRow = min(rowCount, baseLimit)

	// determine if we have spare display slots
	spare := max - (displaySrc + displayConv + displayRow)

	// if we have spare, distribute it in priority order: source, conversion, row
	for _, assign := range []struct {
		count *int
		avail int
	}{
		{&displaySrc, srcCount},
		{&displayConv, convCount},
		{&displayRow, rowCount},
	} {
		if spare == 0 {
			break
		}

		avail := assign.avail - *assign.count
		take := min(avail, spare)
		*assign.count += take
		spare -= take
	}

	return
}
