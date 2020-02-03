package http

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/bufpool"

	"github.com/valyala/fasthttp"
)

const (
	queryKeyN = "n"
)

var (
	methodGet  = []byte("GET")
	methodPost = []byte("POST")
	uriLog     = []byte("/log/")
)

// APIHTTP is an HTTP API instance
type APIHTTP struct {
	eventLog eventlog.EventLog
	server   *fasthttp.Server
	bufPool  *bufpool.Pool
}

// NewAPIHTTP returns a new HTTP API
func NewAPIHTTP(eventLog eventlog.EventLog) *APIHTTP {
	if eventLog == nil {
		panic(errors.New("missing eventlog on APIHTTP init"))
	}
	api := &APIHTTP{
		eventLog: eventLog,
		bufPool:  bufpool.NewPool(64),
	}
	api.server = &fasthttp.Server{
		Handler: api.handle,
	}
	return api
}

// Serve serves incoming connections from the given listener
func (api *APIHTTP) Serve(ln net.Listener) error {
	return api.server.Serve(ln)
}

// ListenAndServe serves incoming connections
// from a new listener on the given address
func (api *APIHTTP) ListenAndServe(addr string) error {
	if addr == "" {
		addr = ":http"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("init listener: %w", err)
	}
	return api.server.Serve(ln)
}

// Shutdown gracefully shuts down the server
// without interrupting any active connections
func (api *APIHTTP) Shutdown() error {
	return api.server.Shutdown()
}

// parseQueryN parses the "n" query parameter from the given request context
// returning false if the request processing shouldn't be continued
func parseQueryN(ctx *fasthttp.RequestCtx) (uint64, bool) {
	args := ctx.QueryArgs()
	b := args.Peek(queryKeyN)
	if b == nil {
		return 0, true
	}

	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusBadRequest),
			fasthttp.StatusBadRequest,
		)
		return 0, false
	}

	return n, true
}
