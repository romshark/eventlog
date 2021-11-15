package main

import (
	"fmt"
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
	http cli.ConfHTTP,
) error {
	// Initialize the API server
	server := apifasthttp.New(logErr, eventlog, int(http.MaxScanBatchSize))
	httpServer := &fasthttp.Server{
		Handler:     server.Serve,
		ReadTimeout: http.ReadTimeout,
	}

	cerr := make(chan error, 1)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Launch server
	go func() {
		logInfo.Printf("listening on %s", http.Host)
		cerr <- httpServer.ListenAndServe(http.Host)
	}()

	// Await stop or listener error
	select {
	case <-stop:
		logInfo.Println("shutting down...")
		if err := httpServer.Shutdown(); err != nil {
			return fmt.Errorf("shutting down http server: %s", err)
		}

		server.Close()

	case err := <-cerr:
		return fmt.Errorf("listening: %w", err)
	}

	logInfo.Println("closing eventlog...")
	if err := eventlog.Close(); err != nil {
		return fmt.Errorf("closing eventlog: %w", err)
	}

	logInfo.Println("shutdown")
	return nil
}
