package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/inmem"
)

func main() {
	lInfo := log.New(os.Stdout, "", log.LstdFlags)
	lErr := log.New(os.Stderr, "ERR", log.LstdFlags)
	if err := cli.Run(
		os.Args,
		&App{logInfo: lInfo, logErr: lErr},
		os.Stdout,
		os.Stderr,
	); err != nil {
		lErr.Fatal(err)
	}
}

type App struct {
	logInfo *log.Logger
	logErr  *log.Logger
}

func (a *App) HandleInmem(confHTTP cli.ConfHTTP, meta map[string]string) error {
	eventLog := eventlog.New(inmem.New(meta))
	return launchAPIFastHTTP(a.logInfo, a.logErr, eventLog, confHTTP)
}

func (a *App) HandleRun(path string, confHTTP cli.ConfHTTP) error {
	f, err := file.Open(path)
	if err != nil {
		return fmt.Errorf("opening database file: %w", err)
	}

	eventLog := eventlog.New(f)
	return launchAPIFastHTTP(a.logInfo, a.logErr, eventLog, confHTTP)
}

func (a *App) HandleCreate(path string, meta map[string]string) error {
	if err := file.Create(path, meta, 0777); err != nil {
		return fmt.Errorf("creating database file: %w", err)
	}
	return nil
}

func (a *App) HandleCheck(path string, quiet bool) error {
	var fileSize int64
	if info, err := os.Stat(path); os.IsNotExist(err) {
		// File doesn't exist, skip check
		return fmt.Errorf("database file not found")
	} else if err != nil {
		return fmt.Errorf("reading database file info: %w", err)
	} else {
		fileSize = info.Size()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stop:
			// Cancel integrity check
			cancel()
			a.logInfo.Println("canceling integrity check")
		case <-ctx.Done():
		}
	}()

	fl, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening file for integrity check: %w", err)
	}
	defer fl.Close()

	onEvent := func(
		checksum uint64,
		e eventlog.Event,
	) error {
		if ctx.Err() != nil {
			return err
		}

		// Integrity check progress
		a.logInfo.Printf(
			"%.2f: Valid entry at offset %d\n"+
				" time:            %s\n"+
				" label:           %q\n"+
				" payload (bytes): %d\n"+
				" checksum:        %d\n",
			float64(e.Version)/float64(fileSize)*100,
			e.Version,
			time.Unix(int64(e.Timestamp), 0),
			string(e.Label),
			len(e.PayloadJSON),
			checksum,
		)
		return nil
	}

	if quiet {
		onEvent = func(checksum uint64, e eventlog.Event) error {
			return ctx.Err()
		}
	}

	if err := file.CheckIntegrity(
		make([]byte, file.MinReadBufferLen),
		&FileOffsetLenReader{fl},
		onEvent,
	); err != nil {
		return fmt.Errorf("checking file integrity: %w", err)
	}

	return nil
}

type FileOffsetLenReader struct{ *os.File }

func (r *FileOffsetLenReader) Len() (uint64, error) {
	fi, err := r.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size()), nil
}
