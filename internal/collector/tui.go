package collector

import (
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/turbot/tailpipe/internal/parquet"
)

type collectionModel struct {
	// status
	status status

	// cancelled
	cancelled bool
}

type CollectionFinishedMsg struct {
	status status
}

type AwaitingCompactionMsg struct{}

type CollectionStatusUpdateMsg struct {
	status status
}

type CollectionErrorsMsg struct {
	errorFilePath string
	errors        []string
}

func newCollectionModel(status status) collectionModel {
	return collectionModel{
		status: status,
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
		c.status = t.status
		return c, tea.Quit
	case status:
		c.status = t
		return c, nil
	case CollectionStatusUpdateMsg:
		c.status = t.status
		return c, nil
	case AwaitingCompactionMsg:
		// this doesn't do anything useful except trigger a view update with file compaction placeholder
		cs := parquet.CompactionStatus{}
		c.status.compactionStatus = &cs
		return c, nil
	}
	return c, nil
}

func (c collectionModel) View() string {
	var out strings.Builder
	header := c.status.CollectionHeader()
	body := c.status.String()

	out.WriteString(header)
	out.WriteString(body)

	return out.String()
}
