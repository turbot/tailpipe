package query

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	table table.Model
}

// DisplayResultTable will fetch data from sql.Rows and prepare it for Bubble Tea's table.
func DisplayResultTable(ctx context.Context, rows *sql.Rows) error {
	bubbleColumns, bubbleRows, err := getRowsAndColumns(rows)
	if err != nil {
		return err
	}

	t := table.New(
		table.WithColumns(bubbleColumns),
		table.WithRows(bubbleRows),
		table.WithHeight(len(bubbleRows)+1),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	// Disable selection by making the selected style identical to the normal row style
	s.Selected = s.Cell
	t.SetStyles(s)

	m := model{t}

	_, err = tea.NewProgram(m).Run()

	return err
}

func getRowsAndColumns(rows *sql.Rows) ([]table.Column, []table.Row, error) {
	// Create table columns from SQL column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	// Define the column headers for the Bubble Tea table
	var bubbleColumns []table.Column
	for _, colName := range columns {
		bubbleColumns = append(bubbleColumns, table.Column{
			Title: fmt.Sprintf("%s", colName),
			Width: 10,
		})
	}

	// Define the rows for the table
	var bubbleRows []table.Row

	// Add rows from the query result
	for rows.Next() {
		// Create a slice to hold the values for each row
		columnsData := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))

		// Fill columnPointers with pointers to each item in columnsData
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		// Scan the current row into columnPointers
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, nil, err
		}

		// Convert the column values to strings
		rowData := make([]string, len(columns))
		for colIndex, colValue := range columnsData {
			rowData[colIndex] = fmt.Sprintf("%v", colValue)
		}

		// Append the row data to the Bubble Tea rows
		bubbleRows = append(bubbleRows, table.Row(rowData))
	}

	// Handle errors encountered during iteration
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return bubbleColumns, bubbleRows, nil
}

// Init initializes the Bubble Tea model.
func (m model) Init() tea.Cmd {
	return nil
}

// Update handles updating the Bubble Tea model based on messages.
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {

	return m, tea.Quit

	//switch msg := msg.(type) {
	//case tea.KeyMsg:
	//	switch msg.String() {
	//	case "q", "esc":
	//		return m, tea.Quit // Quit on "q" or "esc" key
	//	}
	//}
	//
	//// Pass messages to the table for navigation
	//var cmd tea.Cmd
	//m.table, cmd = m.table.Update(msg)
	//return m, cmd
}

// View renders the Bubble Tea table.
func (m model) View() string {
	return baseStyle.Render(m.table.View()) + "\n"
}
