package cli

import "errors"

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
	flags.Parse(args[1:])

	m.Verbose = *verbose

	return m, nil
}
