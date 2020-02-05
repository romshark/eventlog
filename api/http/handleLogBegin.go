package http

import (
	"github.com/romshark/eventlog/internal/hex"
	"github.com/valyala/fasthttp"
)

var (
	partBegin1 = []byte(`{"offset":"`)
	partBegin2 = []byte(`"}`)
)

func (api *APIHTTP) handleBegin(ctx *fasthttp.RequestCtx) error {
	beginOffset := api.eventLog.FirstOffset()
	_, _ = ctx.Write(partBegin1)
	_, _ = hex.WriteUint64(ctx, beginOffset)
	_, _ = ctx.Write(partBegin2)
	return nil
}
