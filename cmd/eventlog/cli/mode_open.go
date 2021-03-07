package cli

import (
	"errors"
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
	flagHTTPHost, flagHTTPReadTimeout := httpArgs(flags)
	flags.Parse(args[1:])

	m.HTTP.Host = *flagHTTPHost
	m.HTTP.ReadTimeout = *flagHTTPReadTimeout

	return m, nil
}
