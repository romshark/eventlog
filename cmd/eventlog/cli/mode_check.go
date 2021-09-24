package cli

import (
	"errors"
	"fmt"
)

type ModeCheck struct {
	Path    string
	Verbose bool
}

func parseModeCheck(args []string) (m ModeCheck, err error) {
	if len(args) < 1 {
		return m, errors.New("missing path")
	}
	m.Path = args[0]

	flags := newFlagSet()
	verbose := flags.Bool(ParamVerbose, DefaultCheckVerbose, "Print progress")
	if err = flags.Parse(args[1:]); err != nil {
		err = fmt.Errorf("parsing flags: %w", err)
		return
	}

	m.Verbose = *verbose

	return m, nil
}
