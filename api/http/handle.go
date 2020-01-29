package http

import (
	"bytes"
	"log"

	"github.com/valyala/fasthttp"
)

// handle handles incomming requests
func (api *APIHTTP) handle(ctx *fasthttp.RequestCtx) {
	m := ctx.Method()
	p := ctx.Path()

	var handle func(ctx *fasthttp.RequestCtx) error

	switch {
	case bytes.Equal(m, methodGet):
		switch {
		case len(p) > len(uriLog) &&
			bytes.HasPrefix(p, uriLog):
			// GET /log/:offset
			handle = api.handleRead
		}
	case bytes.Equal(m, methodPost):
		switch {
		case bytes.Equal(p, uriLog):
			// POST /log/
			handle = api.handleAppend
		case len(p) > len(uriLog) &&
			bytes.HasPrefix(p, uriLog):
			// POST /log/:assumedVersion
			handle = api.handleAppendCheck
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
