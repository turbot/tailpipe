package collector

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/dustin/go-humanize"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
	"github.com/turbot/tailpipe/internal/parquet"
)

type collectionModel struct {
	partitionName string
	fromTime      row_source.ResolvedFromTime

	// artifacts
	path            string
	discovered      int64
	downloaded      int64
	downloadedBytes int64
	extracted       int64
	errors          int64

	// rows
	rowsReceived  int64
	rowsEnriched  int64
	rowsConverted int64
	rowsErrors    int64

	cancelled bool
	complete  bool
	initiated time.Time

	// compaction
	compactionStatus *parquet.CompactionStatus
}

type CollectionFinishedMsg struct{}

type AwaitingCompactionMsg struct{}

type CompactionStatusUpdateMsg struct {
	status *parquet.CompactionStatus
}

func newCollectionModel(partitionName string, fromTime row_source.ResolvedFromTime) collectionModel {
	return collectionModel{
		partitionName:    partitionName,
		fromTime:         fromTime,
		initiated:        time.Now(),
		compactionStatus: nil,
	}
}

func (c collectionModel) Init() tea.Cmd {
	return nil
}

func (c collectionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch t := msg.(type) {
	case tea.KeyMsg:
		switch t.String() {
		case "ctrl+c":
			c.cancelled = true
			return c, tea.Quit
		}
	case CollectionFinishedMsg:
		c.complete = true
		return c, tea.Quit
	case status:
		c.path = t.LatestArtifactPath
		c.discovered = t.ArtifactsDiscovered
		c.downloaded = t.ArtifactsDownloaded
		c.downloadedBytes = t.ArtifactsDownloadedBytes
		c.extracted = t.ArtifactsExtracted
		c.errors = t.ArtifactErrors
		c.rowsReceived = t.RowsReceived
		c.rowsEnriched = t.RowsEnriched
		c.rowsConverted = t.RowsConverted
		c.rowsErrors = t.Errors
		return c, nil
	case AwaitingCompactionMsg:
		cs := parquet.CompactionStatus{}
		c.compactionStatus = &cs
		return c, nil
	case CompactionStatusUpdateMsg:
		if c.compactionStatus == nil {
			c.compactionStatus = t.status
			return c, nil
		} else {
			c.compactionStatus.Update(*t.status)
			return c, nil
		}
	}
	return c, nil
}

func (c collectionModel) View() string {
	var b strings.Builder

	displayPath := c.path
	timeLabel := "Time:"
	compaction := "Verifying..."

	if c.complete {
		displayPath = ""
		timeLabel = "Completed:"
		compaction = "No files to compact."
	}

	// header
	fromTimeSource := c.fromTime.Source
	if c.fromTime.Source != "" {
		fromTimeSource = fmt.Sprintf("(%s)", c.fromTime.Source)
	}
	b.WriteString(fmt.Sprintf("\nCollecting logs for %s from %s %s\n\n", c.partitionName, c.fromTime.Time.Format(time.DateOnly), fromTimeSource))

	// artifacts
	artifactDescriptionLen := 11
	artifactCountLen := len(humanize.Comma(c.discovered))
	if c.path != "" || c.discovered > 0 {
		if strings.Contains(displayPath, "/") {
			displayPath = displayPath[strings.LastIndex(displayPath, "/")+1:]
		}

		downloadedDisplay := "0B" // Handle negative values gracefully
		if c.downloadedBytes > 0 {
			//nolint:gosec // we will not have a negative value
			downloadedDisplay = strings.ReplaceAll(humanize.Bytes(uint64(c.downloadedBytes)), " ", "")
		}

		b.WriteString("Artifacts:\n")
		b.WriteString(writeCountLine("Discovered:", artifactDescriptionLen, c.discovered, artifactCountLen, &displayPath))
		b.WriteString(writeCountLine("Downloaded:", artifactDescriptionLen, c.downloaded, artifactCountLen, &downloadedDisplay))
		b.WriteString(writeCountLine("Extracted:", artifactDescriptionLen, c.extracted, artifactCountLen, nil))
		if c.errors > 0 {
			b.WriteString(writeCountLine("Errors:", artifactDescriptionLen, c.errors, artifactCountLen, nil))
		}
		b.WriteString("\n")
	}

	// rows
	rowDescriptionLen := 9
	rowCountLen := len(humanize.Comma(c.rowsReceived))
	b.WriteString("Rows:\n")
	b.WriteString(writeCountLine("Received:", rowDescriptionLen, c.rowsReceived, rowCountLen, nil))
	b.WriteString(writeCountLine("Enriched:", rowDescriptionLen, c.rowsEnriched, rowCountLen, nil))
	b.WriteString(writeCountLine("Saved:", rowDescriptionLen, c.rowsConverted, rowCountLen, nil))
	if c.compactionStatus != nil {
		filtered := c.rowsReceived - (c.rowsConverted + c.rowsErrors)
		if filtered > 0 {
			b.WriteString(writeCountLine("Filtered:", rowDescriptionLen, filtered, rowCountLen, nil))
		}
	}
	if c.rowsErrors > 0 {
		b.WriteString(writeCountLine("Errors:", rowDescriptionLen, c.rowsErrors, rowCountLen, nil))
	}
	b.WriteString("\n")

	// compaction
	if c.compactionStatus != nil {
		b.WriteString("Files:\n")
		if c.compactionStatus.Source == 0 && c.compactionStatus.Uncompacted == 0 {
			b.WriteString(fmt.Sprintf("  %s\n", compaction))
		} else {
			b.WriteString(fmt.Sprintf("  Compacted: %d => %d\n", c.compactionStatus.Source+c.compactionStatus.Uncompacted, c.compactionStatus.Dest+c.compactionStatus.Uncompacted))
		}
		b.WriteString("\n")
	}

	// run time
	duration := time.Since(c.initiated)
	b.WriteString(fmt.Sprintf("%s %s\n", timeLabel, utils.HumanizeDuration(duration)))

	return b.String()
}

func writeCountLine(desc string, descLen int, count int64, maxCountLen int, suffix *string) string {
	s := ""
	if suffix != nil {
		s = *suffix
	}
	return fmt.Sprintf("  %-*s %*s %s\n", descLen, desc, maxCountLen, humanize.Comma(count), s)
}
