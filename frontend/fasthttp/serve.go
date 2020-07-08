package fasthttp

import (
	"bytes"
	"log"

	"github.com/valyala/fasthttp"
)

// Serve handles incomming requests from a fasthttp.Server
func (s *Server) Serve(ctx *fasthttp.RequestCtx) {
	m := ctx.Method()
	p := ctx.Path()

	var handle func(ctx *fasthttp.RequestCtx) error

	switch {
	case bytes.Equal(m, methodGet):
		switch {
		case len(p) > len(uriLog) &&
			bytes.HasPrefix(p, uriLog):
			// GET /log/:offset
			handle = s.handleRead
		case bytes.Equal(p, uriBegin):
			// GET /begin
			handle = s.handleBegin
		case bytes.Equal(p, uriVersion):
			// GET /version
			handle = s.handleVersion
		case bytes.Equal(p, uriSubscription):
			// GET /subscription
			handle = s.handleSubscription
		}
	case bytes.Equal(m, methodPost):
		switch {
		case bytes.Equal(p, uriLog):
			// POST /log/
			handle = s.handleAppend
		case len(p) > len(uriLog) &&
			bytes.HasPrefix(p, uriLog):
			// POST /log/:assumedVersion
			handle = s.handleAppendCheck
		}
	default:
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusMethodNotAllowed),
			fasthttp.StatusMethodNotAllowed,
		)
		return
	}

	if handle == nil {
		// No handler selected
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusNotFound),
			fasthttp.StatusNotFound,
		)
		return
	}

	// Handle request
	if err := handle(ctx); err != nil {
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusInternalServerError),
			fasthttp.StatusInternalServerError,
		)
		log.Print("error: ", err)
		return
	}
}
