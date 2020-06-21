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

	// Initialize the event log engine
	var l eventlog.Implementer
	var err error
	switch *flagStoreEngine {
	case engineInmem:
		l, err = engineinmem.NewInmem()
		if err != nil {
			log.Fatal(err)
		}
	case engineFile:
		l, err = enginefile.NewFile(*flagStoreFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer l.(*enginefile.File).Close()
	default:
		log.Fatalf("unsupported engine %q", *flagStoreEngine)
	}

	// Initialize the frontend
	server := ffhttp.New(eventlog.New(l))
	httpServer := &fasthttp.Server{
		Handler: server.Serve,
	}

	// Launch server
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		log.Printf("listening on %s", *flagAPIHTTP)
		if err := httpServer.ListenAndServe(*flagAPIHTTP); err != nil {
			log.Fatal(err)
		}
	}()

	// Await stop
	<-stop

	log.Println("shutting down...")
	if err := httpServer.Shutdown(); err != nil {
		log.Fatalf("shutdown err: %s", err)
	}
	log.Println("shutdown")
}
