package fasthttp

import (
	"github.com/valyala/fasthttp"
)

const (
	partEmptyObj       = "{}"
	partOpen           = `{"`
	partKVSeparator    = `":"`
	partFieldSeparator = `","`
)

// handleMeta handles GET /meta
func (s *Server) handleMeta(ctx *fasthttp.RequestCtx) error {
	_, _ = ctx.WriteString(partOpen)

	ln := s.eventLog.MetadataLen()
	if ln < 1 {
		_, _ = ctx.WriteString(partEmptyObj)
	} else {
		i := 0
		s.eventLog.ScanMetadata(func(field, value string) bool {
			_, _ = ctx.WriteString(field)
			_, _ = ctx.WriteString(partKVSeparator)
			_, _ = ctx.WriteString(value)
			i++
			if i < ln {
				// Not last
				_, _ = ctx.WriteString(partFieldSeparator)
			}
			return true
		})
	}

	_, _ = ctx.WriteString(partCloseBlockText)

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}
