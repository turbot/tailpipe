package metaquery

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/constants"
)

// .help
func doHelp(_ context.Context, _ *HandlerInput) error {
	var commonCmds = []string{constants.CmdHelp, constants.CmdExit}

	commonCmdRows := getMetaQueryHelpRows(commonCmds, false)
	var advanceCmds []string
	for cmd := range metaQueryDefinitions {
		if !slices.Contains(commonCmds, cmd) {
			advanceCmds = append(advanceCmds, cmd)
		}
	}
	advanceCmdRows := getMetaQueryHelpRows(advanceCmds, true)
	// print out
	fmt.Printf("Welcome to Tailpipe shell.\n\nTo start, simply enter your SQL query at the prompt:\n\nCommon commands:\n\n%s\n\nAdvanced commands:\n\n%s\n\nDocumentation available at %s\n\n", //nolint:forbidigo//UI output
		buildTable(commonCmdRows, true),
		buildTable(advanceCmdRows, true),
		pconstants.Bold("https://tailpipe.io/docs"))

	return nil
}

func getMetaQueryHelpRows(cmds []string, arrange bool) [][]string {
	var rows [][]string
	for _, cmd := range cmds {
		metaQuery := metaQueryDefinitions[cmd]
		var argsStr []string
		if len(metaQuery.args) > 2 {
			rows = append(rows, []string{cmd + " " + "[mode]", metaQuery.description})
		} else {
			for _, v := range metaQuery.args {
				argsStr = append(argsStr, v.value)
			}
			rows = append(rows, []string{cmd + " " + strings.Join(argsStr, "|"), metaQuery.description})
		}
	}
	// sort by metacmds name
	if arrange {
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i][0] < rows[j][0]
		})
	}
	return rows
}
