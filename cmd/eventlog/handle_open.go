package main

import (
	"log"

	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog"
	filelog "github.com/romshark/eventlog/eventlog/file"
)

func handleOpen(
	logInfo *log.Logger,
	logErr *log.Logger,
	m cli.ModeOpen,
) {
	f, err := filelog.Open(m.Path[len("file:"):])
	if err != nil {
		logErr.Fatal(err)
	}

	eventLog := eventlog.New(f)
	launchFrontendFastHTTP(logInfo, logErr, eventLog, m.HTTP)
}
