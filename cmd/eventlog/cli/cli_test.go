package cli_test

import (
	"io"
	"strings"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	cli "github.com/romshark/eventlog/cmd/eventlog/cli"
	"github.com/stretchr/testify/require"
)

//go:generate go run github.com/golang/mock/mockgen -destination mock_gen_test.go -package cli_test -source cli.go Executer

func TestRun(t *testing.T) {
	expectNoErr := func(t *testing.T, err error) {
		require.NoError(t, err)
	}

	for _, tt := range []struct {
		in      []string
		prepare func(m *MockExecuter)
		err     func(*testing.T, error)
	}{
		{
			in: []string{"run", "/path/to/file.database"},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleRun("/path/to/file.database", DefaultHTTPConf()).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{
				"run", "/path/to/file",
				"--http-host", "testhost:9090",
				"--http-read-timeout", "5ms",
				"--http-max-scan-batch-size", "2000",
			},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleRun("/path/to/file", cli.ConfHTTP{
						Host:             "testhost:9090",
						ReadTimeout:      5 * time.Millisecond,
						MaxScanBatchSize: 2000,
					}).Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{"inmem"},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleInmem(DefaultHTTPConf(), map[string]string{}).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{
				"inmem",
				"--http-host", "testhost:9090",
				"--http-read-timeout", "5ms",
				"--meta", "foo:bar",
				"--meta", "bazz:fazz",
			},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleInmem(cli.ConfHTTP{
						Host:             "testhost:9090",
						ReadTimeout:      5 * time.Millisecond,
						MaxScanBatchSize: 1000,
					}, map[string]string{
						"foo":  "bar",
						"bazz": "fazz",
					}).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{"check", "/path/to/file"},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleCheck("/path/to/file", false).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{"check", "/path/to/file", "--quiet"},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleCheck("/path/to/file", true).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{"create", "/path/to/file"},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleCreate("/path/to/file", map[string]string{}).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{
				"create",
				"/path/to/file",
				"--meta", "foo:bar",
				"-m", "bazz:fazz",
			},
			prepare: func(m *MockExecuter) {
				m.EXPECT().
					HandleCreate("/path/to/file", map[string]string{
						"foo":  "bar",
						"bazz": "fazz",
					}).
					Times(1).
					Return(error(nil))
			},
			err: expectNoErr,
		},
		{
			in: []string{
				"create",
				"/path/to/file",
				"-m", "foo",
			},
			prepare: func(m *MockExecuter) {},
			err: func(t *testing.T, err error) {
				require.Error(t, err)
				require.Equal(
					t, `invalid metadata flag value: "foo"`, err.Error(),
				)
			},
		},
	} {
		t.Run(strings.Join(tt.in, "_"), func(t *testing.T) {
			c := gomock.NewController(t)
			m := NewMockExecuter(c)
			tt.prepare(m)
			tt.err(t, cli.Run(tt.in, m, io.Discard, io.Discard))
		})
	}
}

func DefaultHTTPConf() cli.ConfHTTP {
	return cli.ConfHTTP{
		Host:             ":8080",
		ReadTimeout:      2 * time.Second,
		MaxScanBatchSize: 1000,
	}
}
