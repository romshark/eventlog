package fasthttp

import (
	"strconv"

	"github.com/romshark/eventlog/eventlog"

	"github.com/valyala/fasthttp"
)

const (
	queryKeyN = "n"
)

var (
	methodGet  = []byte("GET")
	methodPost = []byte("POST")
	uriLog     = []byte("/log/")
	uriBegin   = []byte("/begin")
)

// Server is an HTTP API instance
type Server struct {
	eventLog *eventlog.EventLog
}

// New returns a new HTTP API
func New(eventLog *eventlog.EventLog) *Server {
	return &Server{
		eventLog: eventLog,
	}
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
