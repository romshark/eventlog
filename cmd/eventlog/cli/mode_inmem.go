package cli

type ModeInmem struct {
	HTTP
	MetaFields map[string]string
}

func parseModeInmem(args []string) (m ModeInmem, err error) {
	var metaFields arrayFlag

	flags := newFlagSet()
	flags.Var(&metaFields, "meta", "Metadata fields")
	flagHTTPHost, flagHTTPReadTimeout := httpArgs(flags)
	flags.Parse(args)

	m.HTTP.Host = *flagHTTPHost
	m.HTTP.ReadTimeout = *flagHTTPReadTimeout

	m.MetaFields, err = parseMetaFields(metaFields)
	if err != nil {
		return m, err
	}

	return m, nil
}
