package collector

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbletea"
	"github.com/dustin/go-humanize"

	"github.com/turbot/pipe-fittings/utils"
	"github.com/turbot/tailpipe-plugin-sdk/row_source"
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

	complete          bool
	terminalWidth     int
	initiated         time.Time
	progressBarConfig progress.Model
}

type collectionCompleteMsg struct{}

func newCollectionModel(partitionName string, fromTime row_source.ResolvedFromTime) collectionModel {
	return collectionModel{
		partitionName:     partitionName,
		fromTime:          fromTime,
		initiated:         time.Now(),
		progressBarConfig: progress.New(progress.WithWidth(20), progress.WithFillCharacters('=', '-'), progress.WithColorProfile(3), progress.WithoutPercentage()),
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
	case collectionCompleteMsg:
		c.complete = true
		return c, nil
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
	}
	return c, nil
}

func (c collectionModel) View() string {
	var b strings.Builder
	var countLength int = 5
	var descriptionLength int = 12
	countArtifactsDisplayLen := len(humanize.Comma(c.discovered))
	countRowsDisplayLen := len(humanize.Comma(c.rowsReceived))
	downloadedDisplay := fmt.Sprintf("(%s)", humanize.Bytes((uint64)(c.downloadedBytes)))
	if c.rowsReceived > c.discovered {
		descriptionLength = 11
	}
	if countArtifactsDisplayLen > countLength {
		countLength = countArtifactsDisplayLen
	}
	if countRowsDisplayLen > countLength {
		countLength = countRowsDisplayLen
	}

	// header
	b.WriteString(fmt.Sprintf("Collecting logs for %s from %s (%s)\n\n", c.partitionName, c.fromTime.Time.Format("2006-01-02"), c.fromTime.Source))

	// artifacts
	if c.path != "" || c.discovered > 0 {
		b.WriteString("Artifacts:\n")
		b.WriteString(writeCountLine("Discovered:", descriptionLength, c.discovered, countLength, &c.path))
		if c.complete {
			b.WriteString(writeCountLine("Downloaded:", descriptionLength, c.downloaded, countLength, &downloadedDisplay))
			b.WriteString(writeCountLine("Extracted:", descriptionLength, c.extracted, countLength, nil))
		} else {
			b.WriteString(writeProgressLine("Downloaded:", descriptionLength, c.downloaded, countLength, float64(c.downloaded)/float64(c.discovered), &downloadedDisplay, &c.progressBarConfig))
			b.WriteString(writeProgressLine("Extracted:", descriptionLength, c.extracted, countLength, float64(c.extracted)/float64(c.discovered), nil, &c.progressBarConfig))
		}
		if c.errors > 0 {
			b.WriteString(writeCountLine("Errors:", descriptionLength, c.errors, countLength, nil))
		}
		b.WriteString("\n")
	}

	// rows
	b.WriteString("Rows:\n")
	b.WriteString(writeCountLine("Received:", descriptionLength, c.rowsReceived, countLength, nil))
	if c.complete {
		b.WriteString(writeCountLine("Enriched:", descriptionLength, c.rowsEnriched, countLength, nil))
		b.WriteString(writeCountLine("Converted:", descriptionLength, c.rowsConverted, countLength, nil))
	} else {
		b.WriteString(writeProgressLine("Enriched:", descriptionLength, c.rowsEnriched, countLength, float64(c.rowsEnriched)/float64(c.rowsReceived), nil, &c.progressBarConfig))
		b.WriteString(writeProgressLine("Converted:", descriptionLength, c.rowsConverted, countLength, float64(c.rowsConverted)/float64(c.rowsReceived), nil, &c.progressBarConfig))
	}
	if c.rowsErrors > 0 {
		b.WriteString(writeCountLine("Errors:", descriptionLength, c.rowsErrors, countLength, nil))
	}
	b.WriteString("\n")

	// run time
	duration := time.Since(c.initiated)
	b.WriteString(fmt.Sprintf("Time: %s\n", utils.HumanizeDuration(duration)))

	return b.String()
}

func writeCountLine(desc string, descLen int, count int64, maxCountLen int, suffix *string) string {
	s := ""
	if suffix != nil {
		s = *suffix
	}
	return fmt.Sprintf("  %-*s%*s %s\n", descLen, desc, maxCountLen, humanize.Comma(count), s)
}

func writeProgressLine(desc string, descLen int, count int64, maxCountLen int, percent float64, suffix *string, pb *progress.Model) string {
	s := ""
	if suffix != nil {
		s = *suffix
	}
	if math.IsNaN(percent) {
		percent = 0
	}
	return fmt.Sprintf("  %-*s%*s [%s] %3.0f%% %s\n", descLen, desc, maxCountLen, humanize.Comma(count), pb.ViewAs(percent), math.Floor(percent*100), s)
}
