package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog"
	ffhttp "github.com/romshark/eventlog/frontend/fasthttp"

	"github.com/valyala/fasthttp"
)

func launchFrontendFastHTTP(
	logInfo *log.Logger,
	logErr *log.Logger,
	eventlog *eventlog.EventLog,
	http cli.HTTP,
) {
	// Initialize the frontend
	server := ffhttp.New(logErr, eventlog)
	httpServer := &fasthttp.Server{
		Handler:     server.Serve,
		ReadTimeout: http.ReadTimeout,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Launch server
	go func() {
		logInfo.Printf("listening on %s", http.Host)
		if err := httpServer.ListenAndServe(http.Host); err != nil {
			logErr.Fatal(err)
		}
	}()

	// Await stop
	<-stop

	logInfo.Println("shutting down...")
	if err := httpServer.Shutdown(); err != nil {
		logErr.Printf("shutting down http server: %s", err)
	}

	server.Close()

	logInfo.Println("closing eventlog...")
	if err := eventlog.Close(); err != nil {
		logErr.Fatalf("closing eventlog: %s", err)
	}

	logInfo.Println("shutdown")
}
