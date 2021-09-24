package cli

import "fmt"

type ModeInmem struct {
	HTTP
	MetaFields map[string]string
}

func parseModeInmem(args []string) (m ModeInmem, err error) {
	var metaFields arrayFlag

	flags := newFlagSet()
	flags.Var(&metaFields, "meta", "Metadata fields")
	flagHTTPHost, flagHTTPReadTimeout, flagMaxScanBatchSize := httpArgs(flags)
	if err = flags.Parse(args); err != nil {
		err = fmt.Errorf("parsing flags: %w", err)
		return
	}

	m.HTTP.Host = *flagHTTPHost
	m.HTTP.ReadTimeout = *flagHTTPReadTimeout
	m.HTTP.MaxScanBatchSize = *flagMaxScanBatchSize

	m.MetaFields, err = parseMetaFields(metaFields)
	if err != nil {
		return m, err
	}

	return m, nil
}
