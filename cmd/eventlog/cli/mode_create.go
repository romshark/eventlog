package cli

import (
	"errors"
	"fmt"
)

type ModeCreate struct {
	Path       string
	MetaFields map[string]string
}

func parseModeCreate(args []string) (m ModeCreate, err error) {
	if len(args) < 1 {
		return m, errors.New("missing path")
	}
	m.Path = args[0]

	var metaFields arrayFlag

	flags := newFlagSet()
	flags.Var(&metaFields, "meta", "Metadata fields")
	if err = flags.Parse(args[1:]); err != nil {
		err = fmt.Errorf("parsing flags: %w", err)
		return
	}

	m.MetaFields, err = parseMetaFields(metaFields)
	if err != nil {
		return m, err
	}
	return m, nil
}
