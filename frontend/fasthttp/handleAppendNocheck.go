package fasthttp

import (
	"errors"

	"github.com/valyala/fasthttp"
)

var errSingleEvent = errors.New("abort scan")

// handleAppendNocheck handles POST /log/
func (s *Server) handleAppendNocheck(ctx *fasthttp.RequestCtx) error {
	return handleAppend(
		ctx,
		s.eventLog.Append,
		s.eventLog.AppendMulti,
	)
}
