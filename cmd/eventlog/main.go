package main

import (
	"flag"
	"log"

	apihttp "github.com/romshark/eventlog/api/http"
	"github.com/romshark/eventlog/eventlog"
	enginefile "github.com/romshark/eventlog/eventlog/file"
	engineinmem "github.com/romshark/eventlog/eventlog/inmem"
)

const (
	engineInmem = "inmem"
	engineFile  = "file"
)

var (
	flatAPIHTTP = flag.String(
		"httpapi",
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

	var l eventlog.EventLog
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

	apiHTTP := apihttp.NewAPIHTTP(l)
	if err := apiHTTP.ListenAndServe(*flatAPIHTTP); err != nil {
		log.Fatal(err)
	}
}
