package constants

// Metaquery commands

const (
	//CmdTableList        = ".tables"             // List all tables
	CmdOutput = ".output" // Set output mode
	//CmdTiming           = ".timing"             // Toggle query timer
	CmdHeaders      = ".header"       // Toggle headers output
	CmdSeparator    = ".separator"    // Set the column separator
	CmdExit         = ".exit"         // Exit the interactive prompt
	CmdQuit         = ".quit"         // Alias for .exit
	CmdInspect      = ".inspect"      // inspect
	CmdMulti        = ".multi"        // toggle multi line query
	CmdClear        = ".clear"        // clear the console
	CmdHelp         = ".help"         // list all meta commands
	CmdAutoComplete = ".autocomplete" // enable or disable auto complete
)
