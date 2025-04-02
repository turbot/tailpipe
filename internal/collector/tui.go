package collector

import (
	"strings"
	"time"

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

type tickMsg struct{}

func newCollectionModel(status status) collectionModel {
	return collectionModel{
		status: status,
	}
}

func (c collectionModel) Init() tea.Cmd {
	// start the ticker
	return tickCmd()
}

// Update will handle messages sent to the model and return the new model and optional command
func (c collectionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch t := msg.(type) {
	case tea.KeyMsg:
		switch t.String() {
		case "ctrl+c":
			// cancel the collection & exit
			c.cancelled = true
			return c, tea.Quit
		}
	case CollectionFinishedMsg:
		// set final status and exit
		c.status = t.status
		return c, tea.Quit
	case status:
		// update the status
		c.status = t
		return c, nil
	case CollectionStatusUpdateMsg:
		// update the status
		c.status = t.status
		return c, nil
	case AwaitingCompactionMsg:
		// this doesn't do anything useful except trigger a view update with file compaction placeholder
		cs := parquet.CompactionStatus{}
		c.status.compactionStatus = &cs
		return c, nil
	case tickMsg:
		// if cancelled or complete just return no need to schedule next tick
		if c.cancelled || c.status.complete {
			return c, nil
		}

		// update view & schedule next tick event
		return c, tickCmd()
	}
	return c, nil
}

// View will render the model
func (c collectionModel) View() string {
	var out strings.Builder
	header := c.status.CollectionHeader()
	body := c.status.String()

	out.WriteString(header)
	out.WriteString(body)

	return out.String()
}

// tickCmd returns a command that sends a tick message after specified duration
func tickCmd() tea.Cmd {
	return tea.Tick(time.Second*1, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}
