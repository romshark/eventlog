package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/romshark/eventlog/eventlog/file"
	enginefile "github.com/romshark/eventlog/eventlog/file"
)

func handleCheck(
	logInfo *log.Logger,
	logErr *log.Logger,
	m cli.ModeCheck,
) {
	var fileSize int64
	if info, err := os.Stat(m.Path); os.IsNotExist(err) {
		// File doesn't exist, skip check
		logErr.Fatal("file not found")
	} else if err != nil {
		logErr.Fatalf("reading source file info: %s", err)
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
			logInfo.Println("canceling integrity check")
		case <-ctx.Done():
		}
	}()

	fl, err := os.OpenFile(m.Path, os.O_RDONLY, 0644)
	if err != nil {
		logErr.Fatalf("opening file for integrity check: %s", err)
	}
	defer fl.Close()

	onEvent := func(
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
	}

	if !m.Verbose {
		onEvent = func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			return nil
		}
	}

	if err := enginefile.CheckIntegrity(
		ctx,
		make([]byte, file.MinReadBufferLen),
		fl,
		onEvent,
	); err != nil {
		logErr.Fatalf("checking file integrity: %s", err)
	}
}
