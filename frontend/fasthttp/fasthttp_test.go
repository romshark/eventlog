package fasthttp_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/inmem"
	ffhttp "github.com/romshark/eventlog/frontend/fasthttp"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func TestAppendCheckInvalidVersion(t *testing.T) {
	s := setup(t)
	r := require.New(t)

	resp := request(t, s, func(req *fasthttp.Request) {
		req.URI().SetPath("/log/invalid")
	})

	r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
	r.Equal("ErrInvalidOffset", string(resp.Body()))
}

func TestAppendInvalidPayload(t *testing.T) {
	for _, t1 := range []struct {
		name    string
		payload string
	}{
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
				req.URI().SetPath(fmt.Sprintf("/log/%x", s.DB.FirstOffset()))
			})

			r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
			r.Equal("ErrInvalidPayload", string(resp.Body()))
		})

		t.Run(t1.name, func(t *testing.T) {
			s := setup(t)
			r := require.New(t)

			resp := request(t, s, func(req *fasthttp.Request) {
				// Append
				req.Header.SetMethod("POST")
				req.URI().SetPath("/log/")
			})

			r.Equal(fasthttp.StatusBadRequest, resp.StatusCode())
			r.Equal("ErrInvalidPayload", string(resp.Body()))
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
	i, err := inmem.NewInmem()
	require.NoError(t, err)

	s.DB = eventlog.New(i)
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
		Handler: ffhttp.New(
			log.New(os.Stderr, "ERR", log.LstdFlags),
			s.DB,
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
