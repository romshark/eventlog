package cli_test

import (
	"testing"
	"time"

	"github.com/romshark/eventlog/cmd/eventlog/cli"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		in  []string
		err func(*testing.T, error)
		out interface{}
	}{
		{
			in: []string{"help", "testcmd"},
			out: cli.ModeHelp{
				Command: "testcmd",
			},
		},
		{
			in: []string{"open", "/path/to/file"},
			out: cli.ModeOpen{
				Path: "/path/to/file",
				HTTP: cli.HTTP{
					Host:        cli.DefaultHTTPHost,
					ReadTimeout: cli.DefaultHTTPReadTimeout,
				},
			},
		},
		{
			in: []string{
				"open", "/path/to/file",
				"-http-host", "testhost:9090",
				"-http-read-timeout", "5ms",
			},
			out: cli.ModeOpen{
				Path: "/path/to/file",
				HTTP: cli.HTTP{
					Host:        "testhost:9090",
					ReadTimeout: 5 * time.Millisecond,
				},
			},
		},
		{
			in: []string{"inmem"},
			out: cli.ModeInmem{
				HTTP: cli.HTTP{
					Host:        cli.DefaultHTTPHost,
					ReadTimeout: cli.DefaultHTTPReadTimeout,
				},
				MetaFields: map[string]string{},
			},
		},
		{
			in: []string{
				"inmem",
				"-http-host", "testhost:9090",
				"-http-read-timeout", "5ms",
				"-meta", "foo:bar",
				"-meta", "bazz:fazz",
			},
			out: cli.ModeInmem{
				HTTP: cli.HTTP{
					Host:        "testhost:9090",
					ReadTimeout: 5 * time.Millisecond,
				},
				MetaFields: map[string]string{
					"foo":  "bar",
					"bazz": "fazz",
				},
			},
		},
		{
			in: []string{"check", "path/to/file"},
			out: cli.ModeCheck{
				Path:    "path/to/file",
				Verbose: true,
			},
		},
		{
			in: []string{
				"check", "path/to/file",
				"-verbose=false",
			},
			out: cli.ModeCheck{
				Path:    "path/to/file",
				Verbose: false,
			},
		},
		{
			in: []string{"create", "/path/to/file"},
			out: cli.ModeCreate{
				Path:       "/path/to/file",
				MetaFields: map[string]string{},
			},
		},
		{
			in: []string{
				"create", "/path/to/file",
				"-meta", "foo:bar",
				"-meta", "bazz:fazz",
			},
			out: cli.ModeCreate{
				Path: "/path/to/file",
				MetaFields: map[string]string{
					"foo":  "bar",
					"bazz": "fazz",
				},
			},
		},
		{
			in: []string{"open"},
			err: func(t *testing.T, err error) {
				require.Equal(t, "missing path", err.Error())
			},
		},
		{
			in: []string{"check"},
			err: func(t *testing.T, err error) {
				require.Equal(t, "missing path", err.Error())
			},
		},
		{
			in: []string{"create"},
			err: func(t *testing.T, err error) {
				require.Equal(t, "missing path", err.Error())
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			mode, err := cli.Parse(tt.in)
			if tt.err != nil {
				require.Error(t, err)
				tt.err(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.out, mode)
			}
		})
	}
}
