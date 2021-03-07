package fasthttp

import (
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"

	"github.com/fasthttp/websocket"
)

var (
	methodGet       = []byte("GET")
	methodPost      = []byte("POST")
	uriLog          = []byte("/log/")
	uriMeta         = []byte("/meta")
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

type Log interface {
	Printf(format string, v ...interface{})
}
