package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type Executer interface {
	HandleRun(path string, http ConfHTTP) error
	HandleInmem(http ConfHTTP, meta map[string]string) error
	HandleCreate(path string, meta map[string]string) error
	HandleCheck(path string, quiet bool) error
	HandleVersion(url string) error
}

func Run(osArgs []string, e Executer, wOut, wErr io.Writer) error {
	app := &cobra.Command{
		Use:   "eventlog",
		Short: "A command-line interface for the event database.",
		Long: `Eventlog is a command-line interface for the ` +
			`github.com/romshark/eventlog event database.`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}

	app.AddCommand(
		cmdWithFlags(
			&cobra.Command{
				Use:   "inmem",
				Short: "In-memory eventlog",
				Long: `Runs a volatile in-memory eventlog ` +
					`that will lose all data once the process is terminated.`,
				Aliases: []string{"m"},
				RunE: func(c *cobra.Command, args []string) error {
					m, err := getMeta(c)
					if err != nil {
						return err
					}
					return e.HandleInmem(getConfHTTP(c), m)
				},
			},
			setFlagHTTPHost,
			setFlagHTTPReadTimeout,
			setFlagHTTPMaxScanBatchSize,
			setFlagMeta,
		),
		cmdWithFlags(
			&cobra.Command{
				Use: "create <file>",
				Example: `create /path/to/file.db ` +
					`-m company:ajax ` +
					`-m creation:$(date '+%Y_%m_%d_%H_%M_%S_%s')`,
				Short:   "Database creation",
				Long:    `Creates a new database file.`,
				Aliases: []string{"c"},
				RunE: func(c *cobra.Command, args []string) error {
					m, err := getMeta(c)
					if err != nil {
						return err
					}
					filePath := args[0]
					return e.HandleCreate(filePath, m)
				},
				Args: expectArgs("file"),
			},
			setFlagMeta,
		),
		cmdWithFlags(
			&cobra.Command{
				Use:     "run",
				Short:   "Running database from file",
				Long:    `Opens and runs a database from file`,
				Aliases: []string{"r"},
				RunE: func(c *cobra.Command, args []string) error {
					filePath := args[0]
					return e.HandleRun(filePath, getConfHTTP(c))
				},
				Args: expectArgs("file"),
			},
			setFlagHTTPHost,
			setFlagHTTPReadTimeout,
			setFlagHTTPMaxScanBatchSize,
		),
		cmdWithFlags(
			&cobra.Command{
				Use:   "check",
				Short: "Integrity check",
				Long:  `Opens a database file and checks its integrity`,
				RunE: func(c *cobra.Command, args []string) error {
					filePath := args[0]
					return e.HandleCheck(filePath, getQuiet(c))
				},
			},
			flagQuiet,
		),
		cmdWithFlags(
			&cobra.Command{
				Use:   "version <url>",
				Short: "Database version inspection",
				Long:  `Connects to a database and prints its latest version`,
				RunE: func(c *cobra.Command, args []string) error {
					url := args[0]
					return e.HandleVersion(url)
				},
				Args: expectArgs("url"),
			},
			flagQuiet,
		),
	)

	app.SetOut(wOut)
	app.SetErr(wErr)
	app.SetArgs(osArgs)
	return app.Execute()
}

func cmdWithFlags(
	cmd *cobra.Command,
	flags ...func(*cobra.Command),
) *cobra.Command {
	for _, f := range flags {
		f(cmd)
	}
	return cmd
}

type ConfHTTP struct {
	Host             string
	ReadTimeout      time.Duration
	MaxScanBatchSize int
}

func parseMetaField(s string) (key, value string, err error) {
	p := strings.Split(s, ":")
	if len(p) != 2 {
		err = fmt.Errorf(
			"invalid key value pair %q, must be formatted as: -m key:value",
			s,
		)
		return
	}
	key, value = p[0], p[1]
	return
}

func expectArgs(expect ...string) func(*cobra.Command, []string) error {
	return func(c *cobra.Command, args []string) error {
		if len(args) != len(expect) {
			return fmt.Errorf(
				"unexpected number of arguments (%d), expected: %s",
				len(args), strings.Join(expect, " "),
			)
		}
		return nil
	}
}

func fvInt(v int) *fv           { return &fv{V: v} }
func fvDur(v time.Duration) *fv { return &fv{V: v} }
func fvBool(v bool) *fv         { return &fv{V: v} }
func fvStr(v string) *fv        { return &fv{V: v} }
func fvStrList(v ...string) *fv { return &fv{V: v} }

type fv struct{ V interface{} }

func (f *fv) Type() string   { return "" }
func (f *fv) String() string { return "" }
func (f *fv) Set(s string) (err error) {
	switch v := f.V.(type) {
	case []string:
		f.V = append(v, s)
	case string:
		f.V = s
	case bool:
		f.V, err = strconv.ParseBool(s)
	case int:
		var b int64
		b, err = strconv.ParseInt(s, 10, 32)
		f.V = int(b)
	case time.Duration:
		f.V, err = time.ParseDuration(s)
	default:
		panic(fmt.Errorf("unsupported flag data type: %#v", f.V))
	}
	return
}

func setFlagHTTPHost(c *cobra.Command) {
	c.Flags().AddFlag(&pflag.Flag{
		Name:  "http-host",
		Usage: "HTTP host address and port",
		Value: fvStr(":8080"),
	})
}

func setFlagHTTPReadTimeout(c *cobra.Command) {
	c.Flags().AddFlag(&pflag.Flag{
		Name:  "http-read-timeout",
		Usage: "HTTP host address and port",
		Value: fvDur(2 * time.Second),
	})
}

func setFlagHTTPMaxScanBatchSize(c *cobra.Command) {
	c.Flags().AddFlag(&pflag.Flag{
		Name:  "http-max-scan-batch-size",
		Usage: "scan batch size limit",
		Value: fvInt(1000),
	})
}

func flagQuiet(c *cobra.Command) {
	c.Flags().AddFlag(&pflag.Flag{
		Name:        "quiet",
		Usage:       "quiet mode",
		Value:       fvBool(false),
		NoOptDefVal: "true",
	})
}

func setFlagMeta(c *cobra.Command) {
	c.Flags().AddFlag(&pflag.Flag{
		Name:      "meta",
		Shorthand: "m",
		Usage:     "database metadata fields",
		Value:     fvStrList(),
	})
}

func getConfHTTP(c *cobra.Command) (v ConfHTTP) {
	return ConfHTTP{
		Host: c.Flag("http-host").
			Value.(*fv).V.(string),
		ReadTimeout: c.Flag("http-read-timeout").
			Value.(*fv).V.(time.Duration),
		MaxScanBatchSize: c.Flag("http-max-scan-batch-size").
			Value.(*fv).V.(int),
	}
}

func getMeta(c *cobra.Command) (map[string]string, error) {
	fl := c.Flag("meta").Value.(*fv).V.([]string)
	f := make(map[string]string, len(fl))
	for _, a := range fl {
		{
			k, v, err := parseMetaField(a)
			if err == nil {
				f[k] = v
				continue
			}
		}
		return nil, fmt.Errorf("invalid metadata flag value: %q", a)
	}
	return f, nil
}

func getQuiet(c *cobra.Command) bool {
	return c.Flag("quiet").Value.(*fv).V.(bool)
}
