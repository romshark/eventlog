package main

import (
	"log"
	"os"
	"os/signal"

	apifasthttp "github.com/romshark/eventlog/api/fasthttp"
	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog"

	"github.com/valyala/fasthttp"
)

func launchAPIFastHTTP(
	logInfo *log.Logger,
	logErr *log.Logger,
	eventlog *eventlog.EventLog,
	http cli.HTTP,
) {
	// Initialize the API server
	server := apifasthttp.New(logErr, eventlog, int(http.MaxScanBatchSize))
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
