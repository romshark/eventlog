package cli

import (
	"errors"
	"fmt"
)

type ModeOpen struct {
	HTTP
	Path string
}

func parseModeOpen(args []string) (m ModeOpen, err error) {
	if len(args) < 1 {
		return m, errors.New("missing path")
	}
	m.Path = args[0]

	flags := newFlagSet()
	flagHTTPHost, flagHTTPReadTimeout, flagMaxScanBatchSize := httpArgs(flags)
	if err = flags.Parse(args[1:]); err != nil {
		err = fmt.Errorf("parsing flags: %w", err)
		return
	}

	m.HTTP.Host = *flagHTTPHost
	m.HTTP.ReadTimeout = *flagHTTPReadTimeout
	m.HTTP.MaxScanBatchSize = *flagMaxScanBatchSize

	return m, nil
}
