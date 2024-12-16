package display

import (
	"slices"
	"strings"

	"github.com/turbot/pipe-fittings/printers"
)

type PrintableResource[T printers.Listable] struct {
	Items []T
}

func NewPrintableResource[T printers.Listable](items []T) *PrintableResource[T] {
	return &PrintableResource[T]{
		Items: items,
	}
}

func (p PrintableResource[T]) GetItems() []T {
	return p.Items
}

func (p PrintableResource[T]) GetTable() (*printers.Table, error) {
	var rows []printers.TableRow
	var cols []string

	for _, item := range p.Items {
		row := item.GetListData().GetRow()
		if len(cols) == 0 {
			cols = row.Columns
		}

		cleanRow(*row)

		rows = append(rows, *row)
	}

	if len(rows) == 0 {
		return printers.NewTable(), nil
	}

	sortFunc := func(a, b printers.TableRow) int {
		// sort by column a first, then column b
		modCompare := strings.Compare(a.Cells[0].(string), b.Cells[0].(string))
		if modCompare != 0 {
			return modCompare
		}
		// first column same, compare the second
		return strings.Compare(a.Cells[1].(string), b.Cells[1].(string))
	}

	slices.SortFunc(rows, sortFunc)

	return printers.NewTable().WithData(rows, cols), nil
}

func cleanRow(row printers.TableRow) {
	var charsToRemove = []string{"\t", "\n", "\r"}
	for i, c := range row.Cells {
		str, ok := c.(string)
		if !ok {
			continue
		}

		for _, r := range charsToRemove {
			str = strings.ReplaceAll(str, r, "")
		}

		// do not truncate the final column
		truncate := i != len(row.Cells)-1
		if truncate {
			const maxWidth = 100
			if len(str) > maxWidth {
				str = str[:maxWidth] + "…"
			}
		}
		row.Cells[i] = str
	}
}
