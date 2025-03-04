package metaquery

import (
	pconstants "github.com/turbot/pipe-fittings/v2/constants"
	"github.com/turbot/tailpipe/internal/constants"
)

type metaQueryArg struct {
	value       string
	description string
}

type metaQueryDefinition struct {
	title       string
	description string
	args        []metaQueryArg
	handler     handler
	validator   validator
	completer   completer
}

var metaQueryDefinitions map[string]metaQueryDefinition

func init() {
	metaQueryDefinitions = map[string]metaQueryDefinition{
		constants.CmdHelp: {
			title:       constants.CmdHelp,
			handler:     doHelp,
			validator:   noArgs,
			description: "Show tailpipe help",
		},
		constants.CmdExit: {
			title:       constants.CmdExit,
			handler:     doExit,
			validator:   noArgs,
			description: "Exit from tailpipe terminal",
		},
		constants.CmdQuit: {
			title:       constants.CmdQuit,
			handler:     doExit,
			validator:   noArgs,
			description: "Exit from tailpipe terminal",
		},
		//constants.CmdTableList: {
		//	title:       constants.CmdTableList,
		//	handler:     listTables,
		//	validator:   atMostNArgs(1),
		//	description: "List or describe tables",
		//},
		constants.CmdSeparator: {
			title:       constants.CmdSeparator,
			handler:     setViperConfigFromArg(pconstants.ArgSeparator),
			validator:   exactlyNArgs(1),
			description: "Set csv output separator",
		},
		constants.CmdHeaders: {
			title:       "headers",
			handler:     setHeader,
			validator:   booleanValidator(constants.CmdHeaders, pconstants.ArgHeader, validatorFromArgsOf(constants.CmdHeaders)),
			description: "Enable or disable column headers",
			args: []metaQueryArg{
				{value: pconstants.ArgOn, description: "Turn on headers in output"},
				{value: pconstants.ArgOff, description: "Turn off headers in output"},
			},
			completer: completerFromArgsOf(constants.CmdHeaders),
		},
		constants.CmdMulti: {
			title:       "multi-line",
			handler:     setMultiLine,
			validator:   booleanValidator(constants.CmdMulti, pconstants.ArgMultiLine, validatorFromArgsOf(constants.CmdMulti)),
			description: "Enable or disable multiline mode",
			args: []metaQueryArg{
				{value: pconstants.ArgOn, description: "Turn on multiline mode"},
				{value: pconstants.ArgOff, description: "Turn off multiline mode"},
			},
			completer: completerFromArgsOf(constants.CmdMulti),
		},
		//constants.CmdTiming: {
		//	title:       "timing",
		//	handler:     setTiming,
		//	validator:   validatorFromArgsOf(constants.CmdTiming),
		//	description: "Enable or disable query execution timing",
		//	args: []metaQueryArg{
		//		{value: pconstants.ArgOff, description: "Turn off query timer"},
		//		{value: pconstants.ArgOn, description: "Display time elapsed after every query"},
		//		{value: pconstants.ArgVerbose, description: "Display time elapsed and details of each scan"},
		//	},
		//	completer: completerFromArgsOf(constants.CmdTiming),
		//},
		constants.CmdOutput: {
			title:       constants.CmdOutput,
			handler:     setViperConfigFromArg(pconstants.ArgOutput),
			validator:   composeValidator(exactlyNArgs(1), validatorFromArgsOf(constants.CmdOutput)),
			description: "Set output format: csv, json, table or line",
			args: []metaQueryArg{
				{value: pconstants.OutputFormatJSON, description: "Set output to JSON"},
				{value: pconstants.OutputFormatCSV, description: "Set output to CSV"},
				{value: pconstants.OutputFormatTable, description: "Set output to Table"},
				{value: pconstants.OutputFormatLine, description: "Set output to Line"},
			},
			completer: completerFromArgsOf(constants.CmdOutput),
		},
		constants.CmdInspect: {
			title:       constants.CmdInspect,
			handler:     inspect,
			validator:   atMostNArgs(1),
			description: "View tables & column information",
			completer:   inspectCompleter,
		},
		constants.CmdClear: {
			title:       constants.CmdClear,
			handler:     clearScreen,
			validator:   noArgs,
			description: "Clear the console",
		},
		constants.CmdAutoComplete: {
			title:       "auto-complete",
			handler:     setAutoComplete,
			validator:   booleanValidator(constants.CmdAutoComplete, pconstants.ArgAutoComplete, validatorFromArgsOf(constants.CmdAutoComplete)),
			description: "Enable or disable auto-completion",
			args: []metaQueryArg{
				{value: pconstants.ArgOn, description: "Turn on auto-completion"},
				{value: pconstants.ArgOff, description: "Turn off auto-completion"},
			},
			completer: completerFromArgsOf(constants.CmdAutoComplete),
		},
	}
}
