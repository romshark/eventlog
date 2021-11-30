package fasthttp_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"testing"

	apifasthttp "github.com/romshark/eventlog/api/fasthttp"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/inmem"
	"github.com/romshark/eventlog/internal"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestAppendCheckMalformedVersion(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	resp := request(t, s, func(req *fasthttp.Request) {
		req.URI().SetPath("/log/malformed")
	})

	r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
	r.Equal(internal.StatusMsgErrMalformedVersion, string(resp.Body()))
}

func TestAppendInvalidPayload(t *testing.T) {
	for _, t1 := range []struct {
		name    string
		payload string
	}{
		{"no payload", ""},
		{"primitive string", `"foo"`},
		{"primitive number", `42`},
		{"empty array", `[]`},
		{"array of primitives", `["foo", "bar"]`},
		{"syntax error in array", "[foo]"},
	} {
		t.Run(t1.name, func(t *testing.T) {
			s := setup(t)
			r := require.New(t)

			resp := request(t, s, func(req *fasthttp.Request) {
				// Append check
				req.Header.SetMethod("POST")
				req.URI().SetPath(fmt.Sprintf("/log/%x", s.DB.VersionInitial()))
				req.SetBodyString(t1.payload)
			})

			r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
			r.Equal(internal.StatusMsgErrInvalidPayload, string(resp.Body()))
		})

		t.Run(t1.name, func(t *testing.T) {
			s := setup(t)
			r := require.New(t)

			resp := request(t, s, func(req *fasthttp.Request) {
				// Append
				req.Header.SetMethod("POST")
				req.URI().SetPath("/log/")
				req.SetBodyString(t1.payload)
			})

			r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
			r.Equal(internal.StatusMsgErrInvalidPayload, string(resp.Body()))
		})
	}
}

func request(
	t *testing.T,
	s Setup,
	prepare func(req *fasthttp.Request),
) *fasthttp.Response {
	req := fasthttp.AcquireRequest()
	t.Cleanup(func() { fasthttp.ReleaseRequest(req) })

	resp := fasthttp.AcquireResponse()
	t.Cleanup(func() { fasthttp.ReleaseResponse(resp) })

	req.SetHost("localhost")
	prepare(req)

	require.NoError(t, s.Client.Do(req, resp))
	return resp
}

type Setup struct {
	DB     *eventlog.EventLog
	Server *fasthttp.Server
	Client *fasthttp.Client
}

func setup(t *testing.T) (s Setup) {
	s.DB = eventlog.New(inmem.New(
		file.MaxPayloadLen,
		map[string]string{"name": "testlog"},
	))
	t.Cleanup(func() {
		if err := s.DB.Close(); err != nil {
			panic(fmt.Errorf("closing eventlog: %s", err))
		}
	})

	inMemListener := fasthttputil.NewInmemoryListener()
	t.Cleanup(func() {
		require.NoError(t, inMemListener.Close())
	})

	s.Server = &fasthttp.Server{
		Handler: apifasthttp.New(
			log.New(os.Stderr, "ERR", log.LstdFlags),
			s.DB,
			10,
		).Serve,
	}

	go func() {
		require.NoError(t, s.Server.Serve(inMemListener))
	}()

	s.Client = &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return inMemListener.Dial()
		},
	}
	return
}

func TestAdjustBatchSize(t *testing.T) {
	const NoLimit = 0
	for _, tt := range []struct{ requested, limit, expected int }{
		{requested: NoLimit, limit: NoLimit, expected: NoLimit},
		{requested: NoLimit, limit: 5, expected: 5},
		{requested: 5, limit: NoLimit, expected: 5},
		{requested: 5, limit: 3, expected: 3},
		{requested: 5, limit: 5, expected: 5},
	} {
		t.Run("", func(t *testing.T) {
			a := apifasthttp.AdjustBatchSize(tt.requested, tt.limit)
			require.Equal(t, tt.expected, a)
		})
	}
}
