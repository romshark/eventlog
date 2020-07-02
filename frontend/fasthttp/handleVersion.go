package fasthttp

import (
	"github.com/romshark/eventlog/internal/hex"
	"github.com/valyala/fasthttp"
)

var (
	partVersion1 = []byte(`{"version":"`)
	partVersion2 = []byte(`"}`)
)

func (s *Server) handleVersion(ctx *fasthttp.RequestCtx) error {
	v := s.eventLog.Version()
	_, _ = ctx.Write(partVersion1)
	_, _ = hex.WriteUint64(ctx, v)
	_, _ = ctx.Write(partVersion2)
	return nil
}
