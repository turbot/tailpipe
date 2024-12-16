package constants

import (
	"github.com/thediveo/enumflag/v2"
	"github.com/turbot/pipe-fittings/constants"
)

type QueryOutputMode enumflag.Flag

const (
	QueryOutputModeCsv QueryOutputMode = iota
	QueryOutputModeJson
	QueryOutputModeLine
	QueryOutputModeTable
)

var QueryOutputModeIds = map[QueryOutputMode][]string{
	QueryOutputModeCsv:   {constants.OutputFormatCSV},
	QueryOutputModeJson:  {constants.OutputFormatJSON},
	QueryOutputModeLine:  {constants.OutputFormatLine},
	QueryOutputModeTable: {constants.OutputFormatTable},
}

type PluginOutputMode enumflag.Flag

const (
	PluginOutputModeJson PluginOutputMode = iota
	PluginOutputModePretty
	PluginOutputModePlain
)

var PluginOutputModeIds = map[PluginOutputMode][]string{
	PluginOutputModeJson:   {constants.OutputFormatJSON},
	PluginOutputModePretty: {constants.OutputFormatPretty},
	PluginOutputModePlain:  {constants.OutputFormatPlain},
}

type ConnectOutputMode enumflag.Flag

const (
	ConnectOutputModeJson ConnectOutputMode = iota
	ConnectOutputModeText
)

var ConnectOutputModeIds = map[ConnectOutputMode][]string{
	ConnectOutputModeJson: {constants.OutputFormatJSON},
	ConnectOutputModeText: {constants.OutputFormatText},
}

func FlagValues[T comparable](mappings map[T][]string) []string {
	var res = make([]string, 0, len(mappings))
	for _, v := range mappings {
		res = append(res, v[0])
	}
	return res

}
