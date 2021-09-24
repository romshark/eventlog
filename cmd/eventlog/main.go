package main

import (
	"fmt"
	"log"
	"os"

	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog"
	filelog "github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/inmem"
)

func main() {
	logInfo := log.New(os.Stdout, "", log.LstdFlags)
	logErr := log.New(os.Stderr, "ERR", log.LstdFlags)

	mode, err := cli.Parse(os.Args[1:])
	if err != nil {
		cli.PrintHelp(os.Stdout, "")
		os.Exit(1)
	}

	switch m := mode.(type) {
	case cli.ModeCheck:
		handleCheck(logInfo, logErr, m)

	case cli.ModeCreate:
		if err := filelog.Create(
			m.Path, m.MetaFields, 0777,
		); err != nil {
			logErr.Fatal(err)
		}

	case cli.ModeOpen:
		handleOpen(logInfo, logErr, m)

	case cli.ModeInmem:
		eventLog := eventlog.New(inmem.New(m.MetaFields))
		launchAPIFastHTTP(logInfo, logErr, eventLog, m.HTTP)

	case cli.ModeHelp:
		cli.PrintHelp(os.Stdout, m.Command)

	default:
		panic(fmt.Errorf("unknown mode: %T", mode))
	}
}
