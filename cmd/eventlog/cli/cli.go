package cli

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

const (
	CmdInmem  = "inmem"
	CmdCreate = "create"
	CmdOpen   = "open"
	CmdCheck  = "check"
	CmdHelp   = "help"

	ParamVerbose         = "verbose"
	ParmMeta             = "m"
	ParamHTTPHost        = "http-host"
	ParamHTTPReadTimeout = "http-read-timeout"
)

func Parse(args []string) (mode interface{}, err error) {
	if a := args; len(a) < 1 {
		return nil, fmt.Errorf("missing command")
	}

	switch a := args[0]; a {
	case CmdInmem:
		mode, err = parseModeInmem(args[1:])
	case CmdCreate:
		mode, err = parseModeCreate(args[1:])
	case CmdOpen:
		mode, err = parseModeOpen(args[1:])
	case CmdCheck:
		mode, err = parseModeCheck(args[1:])
	case CmdHelp:
		mode, err = parseModeHelp(args[1:])
	default:
		err = fmt.Errorf(
			"invalid command %q, use help to show all available commands",
			a,
		)
	}
	return
}

func newFlagSet() *flag.FlagSet {
	s := flag.NewFlagSet("", flag.ExitOnError)
	return s
}

func httpArgs(flags *flag.FlagSet) (
	flagHTTPHost *string,
	flagHTTPReadTimeout *time.Duration,
) {
	flagHTTPHost = flags.String(
		ParamHTTPHost,
		DefaultHTTPHost,
		"TCP address to listen to",
	)
	flagHTTPReadTimeout = flags.Duration(
		ParamHTTPReadTimeout,
		DefaultHTTPReadTimeout,
		"read timeout of the HTTP API server",
	)
	return
}

type HTTP struct {
	Host        string
	ReadTimeout time.Duration
}

const (
	DefaultHTTPHost        = ":8080"
	DefaultHTTPReadTimeout = 2 * time.Second
	DefaultCheckVerbose    = true
)

func parseMetaFields(fields []string) (map[string]string, error) {
	f := make(map[string]string)
	for _, a := range fields {
		{
			k, v, err := parseMetaField(a)
			if err == nil {
				f[k] = v
				continue
			}
		}
		return nil, fmt.Errorf("invalid argument: %q", a)
	}
	return f, nil
}

func parseMetaField(s string) (key, value string, err error) {
	p := strings.Split(s, ":")
	if len(p) != 2 {
		err = fmt.Errorf("invalid key value pair %q", p[1])
		return
	}
	key, value = p[0], p[1]
	return
}
