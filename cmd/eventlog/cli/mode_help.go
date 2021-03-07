package cli

import "errors"

type ModeHelp struct {
	Command string
}

func parseModeHelp(args []string) (m ModeHelp, err error) {
	if len(args) < 1 {
		return m, errors.New("missing command name")
	}
	m.Command = args[0]
	return m, nil
}
