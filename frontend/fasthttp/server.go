package fasthttp

import (
	"strconv"
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"

	"github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
)

const (
	queryKeyN = "n"
)

var (
	methodGet       = []byte("GET")
	methodPost      = []byte("POST")
	uriLog          = []byte("/log/")
	uriBegin        = []byte("/begin")
	uriVersion      = []byte("/version")
	uriSubscription = []byte("/subscription")
)

// Server is an HTTP API instance
type Server struct {
	eventLog       *eventlog.EventLog
	logErr         Log
	wsUpgrader     websocket.FastHTTPUpgrader
	wsPingInterval time.Duration
	wsWriteTimeout time.Duration

	lock    sync.Mutex
	wsConns map[*websocket.Conn]func()
}

// New returns a new HTTP API server instance
func New(
	logErr Log,
	eventLog *eventlog.EventLog,
) *Server {
	return &Server{
		eventLog:       eventLog,
		logErr:         logErr,
		wsUpgrader:     websocket.FastHTTPUpgrader{},
		wsPingInterval: 30 * time.Second,
		wsWriteTimeout: 1 * time.Second,
		wsConns:        map[*websocket.Conn]func(){},
	}
}

// Close closes all open websocket connections
func (s *Server) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, close := range s.wsConns {
		close()
	}
	s.wsConns = map[*websocket.Conn]func(){}
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

type Log interface {
	Printf(format string, v ...interface{})
}
