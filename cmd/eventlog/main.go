package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/romshark/eventlog/eventlog"
	enginefile "github.com/romshark/eventlog/eventlog/file"
	engineinmem "github.com/romshark/eventlog/eventlog/inmem"
	ffhttp "github.com/romshark/eventlog/frontend/fasthttp"

	"github.com/valyala/fasthttp"
)

const (
	engineInmem = "inmem"
	engineFile  = "file"
)

var (
	flagAPIHTTP = flag.String(
		"apihttp",
		":8080",
		"TCP address to listen to",
	)
	flagStoreEngine = flag.String(
		"store",
		engineFile,
		"storage engine",
	)
	flagStoreFilePath = flag.String(
		"storage",
		"./eventlog.db",
		"event storage file path",
	)
)

func main() {
	flag.Parse()

	logInfo := log.New(os.Stdout, "", log.LstdFlags)
	logErr := log.New(os.Stderr, "ERR", log.LstdFlags)

	// Initialize the event log engine
	var eventLogImpl eventlog.Implementer
	var err error
	switch *flagStoreEngine {
	case engineInmem:
		eventLogImpl, err = engineinmem.NewInmem()
		if err != nil {
			logErr.Fatal(err)
		}
	case engineFile:
		eventLogImpl, err = enginefile.NewFile(*flagStoreFilePath)
		if err != nil {
			logErr.Fatal(err)
		}
		defer eventLogImpl.(*enginefile.File).Close()
	default:
		logErr.Fatalf("unsupported engine %q", *flagStoreEngine)
	}

	eventLog := eventlog.New(eventLogImpl)

	// Initialize the frontend
	server := ffhttp.New(logErr, eventLog)
	httpServer := &fasthttp.Server{
		Handler: server.Serve,
	}

	// Launch server
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		logInfo.Printf("listening on %s", *flagAPIHTTP)
		if err := httpServer.ListenAndServe(*flagAPIHTTP); err != nil {
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
	if err := eventLog.Close(); err != nil {
		logErr.Fatalf("closing eventlog: %s", err)
	}

	logInfo.Println("shutdown")
}
