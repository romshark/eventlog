package cli

import (
	"fmt"
	"io"
)

type section struct {
	Title   string
	Content interface{}
}

type list []interface{}

func print(
	w io.Writer,
	indentStr string,
	indent int,
	x interface{},
) {
	printIdent := func(l int) {
		for i := 0; i < l; i++ {
			fmt.Fprint(w, indentStr)
		}
	}

	switch x := x.(type) {
	case string:
		printIdent(indent)
		fmt.Fprintln(w, x)
	case list:
		for _, v := range x {
			print(w, indentStr, indent, v)
		}
	case section:
		printIdent(indent)
		fmt.Fprintln(w, x.Title)
		print(w, indentStr, indent+1, x.Content)
	default:
		panic(fmt.Errorf("unsupported print item type: %T", x))
	}
}

var helpOptMeta = section{
	Title: fmt.Sprintf("-%s '<key>:<value>'", ParamMeta),
	Content: "Appends a new metadata key-value pair separated by a colon " +
		"to the immutable header of the log file. " +
		"Can be used multiple times.",
}
var helpOptHTTPHost = section{
	Title: fmt.Sprintf("-%s <host>[:port]", ParamHTTPHost),
	Content: list{
		"Defines the HTTP API host address.",
		fmt.Sprintf("Default: '%s'", DefaultHTTPHost),
	},
}
var helpOptHTTPMaxScanBatchSize = section{
	Title: fmt.Sprintf("-%s <uint>", ParamHTTPMaxScanBatchSize),
	Content: list{
		"Defines the HTTP API scan batch size limit.",
		fmt.Sprintf("Default: '%d'", DefaultMaxScanBatchSize),
	},
}
var helpOptHTTPReadTimeout = section{
	Title: fmt.Sprintf("-%s <time>", ParamHTTPReadTimeout),
	Content: list{
		"Defines the read-timeout duration for the HTTP API.",
		fmt.Sprintf("Default: '%s'", DefaultHTTPReadTimeout),
	},
}
var helpOptVerbose = section{
	Title: fmt.Sprintf("-%s", ParamVerbose),
	Content: list{
		"Enables verbose mode.",
		fmt.Sprintf("Default: %t", DefaultCheckVerbose),
	},
}
var helpCmdCreate = list{
	fmt.Sprintf("usage: eventlog %s <path> <parameters>", CmdCreate),
	"",
	"Creates a new file-based event log at the given path.",
	"",
	section{
		Title:   "parameters:",
		Content: helpOptMeta,
	},
}
var helpCmdOpen = list{
	fmt.Sprintf("usage: eventlog %s <path> <parameters>", CmdOpen),
	"",
	"Loads an event log from a file " +
		"and starts listening on the HTTP API.",
	"",
	section{
		Title: "parameters:",
		Content: list{
			helpOptHTTPHost,
			helpOptHTTPReadTimeout,
			helpOptHTTPMaxScanBatchSize,
		},
	},
}
var helpCmdCheck = list{
	fmt.Sprintf("usage: eventlog %s <path> <parameters>", CmdCheck),
	"",
	"Performs an integrity check on the given event log file.",
	"",
	section{"parameters:", list{
		helpOptVerbose,
	}},
}
var helpCmdInmem = list{
	fmt.Sprintf("usage: eventlog %s <parameters>", CmdInmem),
	"",
	"Starts a volatile in-memory event log " +
		"listening on the HTTP API.",
	"",
	section{
		Title: "parameters:",
		Content: list{
			helpOptMeta,
			helpOptHTTPHost,
			helpOptHTTPReadTimeout,
		},
	},
}
var helpDefaultHelp = list{
	"usage: eventlog <command> [<parameters>]",
	section{
		Title: "available commands:",
		Content: list{
			CmdCreate,
			CmdOpen,
			CmdCheck,
			CmdInmem,
		},
	},
}

func PrintHelp(w io.Writer, command string) {
	print(w, "  ", 0, func() interface{} {
		switch command {
		case CmdCreate:
			return helpCmdCreate
		case CmdOpen:
			return helpCmdOpen
		case CmdCheck:
			return helpCmdCheck
		case CmdInmem:
			return helpCmdInmem
		}
		return helpDefaultHelp
	}())
}
