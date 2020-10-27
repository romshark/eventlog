package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
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
	flagAPIHTTPReadTimeout = flag.Duration(
		"apihttp-read-timeout",
		2*time.Second,
		"read timeout of the HTTP API server",
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
	flagCheckIntegrity = flag.Bool(
		"check-integrity",
		false,
		"perform file integrity check on launch",
	)
)

func main() {
	flag.Parse()

	logInfo := log.New(os.Stdout, "", log.LstdFlags)
	logErr := log.New(os.Stderr, "ERR", log.LstdFlags)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Initialize the event log engine
	var eventLogImpl eventlog.Implementer
	var err error
	switch *flagStoreEngine {
	case engineInmem:
		eventLogImpl = engineinmem.New()
	case engineFile:
		func() {
			if !*flagCheckIntegrity {
				// Skip file integrity check
				return
			}

			var fileSize int64
			if info, err := os.Stat(*flagStoreFilePath); os.IsNotExist(err) {
				// File doesn't exist, skip check
				return
			} else if err != nil {
				logErr.Fatalf("reading source file info: %s", err)
			} else {
				fileSize = info.Size()
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				select {
				case <-stop:
					// Cancel integrity check
					cancel()
					logInfo.Println("canceling integrity check")
				case <-ctx.Done():
				}
			}()

			fl, err := os.OpenFile(*flagStoreFilePath, os.O_RDONLY, 0644)
			if err != nil {
				logErr.Fatalf("opening file for integrity check: %s", err)
			}
			defer fl.Close()

			if err := enginefile.CheckIntegrity(
				ctx,
				make([]byte, file.MinReadBufferLen),
				fl,
				func(
					offset int64,
					checksum uint64,
					timestamp uint64,
					label []byte,
					payload []byte,
				) error {
					// Integrity check progress
					logInfo.Printf(
						"%.2f: Valid entry at offset %d\n"+
							" time:            %s\n"+
							" label:           %q\n"+
							" payload (bytes): %d\n"+
							" checksum:        %d\n",
						float64(offset)/float64(fileSize)*100,
						offset,
						time.Unix(int64(timestamp), 0),
						string(label),
						len(payload),
						checksum,
					)
					return nil
				},
			); err != nil {
				logErr.Fatalf("checking file integrity: %s", err)
			}
		}()

		eventLogImpl, err = enginefile.New(*flagStoreFilePath)
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
		Handler:     server.Serve,
		ReadTimeout: *flagAPIHTTPReadTimeout,
	}

	// Launch server
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
