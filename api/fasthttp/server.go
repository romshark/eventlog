package fasthttp

import (
	"sync"
	"time"

	"github.com/romshark/eventlog/eventlog"

	"github.com/fasthttp/websocket"
)

// Server is an HTTP API instance
type Server struct {
	eventLog         *eventlog.EventLog
	logErr           Log
	wsUpgrader       websocket.FastHTTPUpgrader
	wsPingInterval   time.Duration
	wsWriteTimeout   time.Duration
	maxReadBatchSize int

	lock    sync.Mutex
	wsConns map[*websocket.Conn]func()
}

// New returns a new HTTP API server instance
func New(
	logErr Log,
	eventLog *eventlog.EventLog,
	maxReadBatchSize int,
) *Server {
	return &Server{
		eventLog:         eventLog,
		logErr:           logErr,
		wsUpgrader:       websocket.FastHTTPUpgrader{},
		wsPingInterval:   30 * time.Second,
		wsWriteTimeout:   1 * time.Second,
		maxReadBatchSize: maxReadBatchSize,
		wsConns:          map[*websocket.Conn]func(){},
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
