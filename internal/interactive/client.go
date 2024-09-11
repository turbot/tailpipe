package interactive

import (
	"context"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.org/x/term"
	"os"
	"strings"
)

type InteractiveClient struct {
	app        *tview.Application
	inputField *tview.InputField
}

func NewInteractiveClient() (*InteractiveClient, error) {

	// how big is the terminal?
	w, _, err := term.GetSize(int(os.Stdin.Fd()))
	if err != nil {
		return nil, err
	}

	words := []string{"hello", "world", "how", "are", "you"}
	app := tview.NewApplication()
	inputField := tview.NewInputField().
		SetLabel("> ").
		SetFieldWidth(w).
		SetDoneFunc(func(key tcell.Key) {
			app.Stop()
		})

	inputField.SetAutocompleteFunc(func(currentText string) (entries []string) {
		if len(currentText) == 0 {
			return
		}
		for _, word := range words {
			if strings.HasPrefix(strings.ToLower(word), strings.ToLower(currentText)) {
				entries = append(entries, word)
			}
		}
		if len(entries) <= 1 {
			entries = nil
		}
		return
	})
	inputField.SetAutocompletedFunc(func(text string, index, source int) bool {
		if source != tview.AutocompletedNavigate {
			inputField.SetText(text)
		}
		return source == tview.AutocompletedEnter || source == tview.AutocompletedClick
	})

	c := &InteractiveClient{
		app:        app,
		inputField: inputField,
	}

	return c, nil
}

func (c *InteractiveClient) Run(context.Context) error {
	if err := c.app.EnableMouse(true).SetRoot(c.inputField, false).Run(); err != nil {
		return err
	}
	return nil
}
