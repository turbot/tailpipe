package collector

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbletea"
	"github.com/dustin/go-humanize"

	"github.com/turbot/pipe-fittings/utils"
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
			// TODO: Handle graceful exit
			return c, tea.Quit
		}
	case CollectionFinishedMsg:
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
		c.complete = true
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
	var countLength int = 5
	var descriptionLength int = 12
	var downloadedDisplay string

	countArtifactsDisplayLen := len(humanize.Comma(c.discovered))
	countRowsDisplayLen := len(humanize.Comma(c.rowsReceived))
	if c.downloadedBytes < 0 {
		downloadedDisplay = "0 B" // Handle negative values gracefully
	} else {
		downloadedDisplay = humanize.Bytes(uint64(c.downloadedBytes))
	}
	if countArtifactsDisplayLen > countLength {
		countLength = countArtifactsDisplayLen
	}
	if countRowsDisplayLen > countLength {
		countLength = countRowsDisplayLen
		descriptionLength = 11
	}

	collectionComplete := c.complete || c.compactionStatus != nil
	displayPath := c.path
	timeLabel := "Time:"

	if collectionComplete {
		// TODO: #tactical we should clear path in event once complete
		displayPath = ""
		timeLabel = "Completed:"
	}

	// header
	b.WriteString(fmt.Sprintf("\nCollecting logs for %s from %s (%s)\n\n", c.partitionName, c.fromTime.Time.Format("2006-01-02"), c.fromTime.Source))

	// artifacts
	if c.path != "" || c.discovered > 0 {
		if strings.Contains(displayPath, "/") {
			displayPath = displayPath[strings.LastIndex(displayPath, "/")+1:]
		}

		b.WriteString("Artifacts:\n")
		b.WriteString(writeCountLine("Discovered:", descriptionLength, c.discovered, countLength, &displayPath))
		b.WriteString(writeCountLine("Downloaded:", descriptionLength, c.downloaded, countLength, &downloadedDisplay))
		b.WriteString(writeCountLine("Extracted:", descriptionLength, c.extracted, countLength, nil))
		if c.errors > 0 {
			b.WriteString(writeCountLine("Errors:", descriptionLength, c.errors, countLength, nil))
		}
		b.WriteString("\n")
	}

	// rows
	b.WriteString("Rows:\n")
	b.WriteString(writeCountLine("Received:", descriptionLength, c.rowsReceived, countLength, nil))
	b.WriteString(writeCountLine("Enriched:", descriptionLength, c.rowsEnriched, countLength, nil))
	b.WriteString(writeCountLine("Converted:", descriptionLength, c.rowsConverted, countLength, nil))
	if c.rowsErrors > 0 {
		b.WriteString(writeCountLine("Errors:", descriptionLength, c.rowsErrors, countLength, nil))
	}
	b.WriteString("\n")

	// compaction
	if c.compactionStatus != nil {
		b.WriteString("File Compaction:\n")
		b.WriteString(fmt.Sprintf("  Compacted: %d => %d\n", c.compactionStatus.Source, c.compactionStatus.Dest))
		b.WriteString(fmt.Sprintf("  Skipped:   %d\n", c.compactionStatus.Uncompacted))
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
	return fmt.Sprintf("  %-*s%*s %s\n", descLen, desc, maxCountLen, humanize.Comma(count), s)
}
