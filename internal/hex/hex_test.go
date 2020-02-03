package hex_test

import (
	"fmt"
	"math"
	"net"
	"testing"

	"github.com/romshark/eventlog/internal/hex"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func newSetup(handler fasthttp.RequestHandler) (
	clt *fasthttp.Client,
	teardown func(),
) {
	ln := fasthttputil.NewInmemoryListener()

	api := fasthttp.Server{
		Handler: handler,
	}
	go func() {
		if err := api.Serve(ln); err != nil {
			panic(err)
		}
	}()

	teardown = func() {
		ln.Close()
	}

	clt = &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
	}
	return
}

func TestWrite(t *testing.T) {
	for _, tt := range []struct {
		in  uint64
		out string
	}{
		{0, "0"},
		{2, "2"},
		{10, "a"},
		{15, "f"},
		{1_024, "400"},
		{16_384, "4000"},
		{65_536, "10000"},
		{1_048_576, "100000"},
		{math.MaxUint64, "ffffffffffffffff"},
		{18_364_758_544_493_064_720, "fedcba9876543210"},
	} {
		t.Run(fmt.Sprintf("%d->%q", tt.in, tt.out), func(t *testing.T) {
			clt, teardown := newSetup(func(ctx *fasthttp.RequestCtx) {
				_, err := hex.WriteUint64(ctx, tt.in)
				if err != nil {
					panic(err)
				}
			})
			defer teardown()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseResponse(resp)

			req := fasthttp.AcquireRequest()
			defer fasthttp.ReleaseRequest(req)
			req.SetHost("test")
			require.NoError(t, clt.Do(req, resp))

			require.Equal(t, tt.out, string(resp.Body()))
		})
	}
}

func TestRead(t *testing.T) {
	for _, tt := range []struct {
		in  string
		out uint64
	}{
		{"0", 0},
		{"2", 2},
		{"a", 10},
		{"f", 15},
		{"400", 1_024},
		{"4000", 16_384},
		{"10000", 65_536},
		{"100000", 1_048_576},
		{"ffffffffffffffff", math.MaxUint64},
		{"fedcba9876543210", 18_364_758_544_493_064_720},
	} {
		t.Run(fmt.Sprintf("%q->%d", tt.in, tt.out), func(t *testing.T) {
			n, err := hex.ReadUint64([]byte(tt.in))
			require.NoError(t, err)
			require.Equal(t, tt.out, n)
		})
	}
}
