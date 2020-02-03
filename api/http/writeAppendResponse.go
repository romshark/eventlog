package http

import (
	"time"

	"github.com/romshark/eventlog/internal/bufpool"
	"github.com/romshark/eventlog/internal/hex"
	"github.com/valyala/fasthttp"
)

var (
	handleAppendPart1 = []byte(`{"offset":"`)
	handleAppendPart2 = []byte(`","newVersion":"`)
	handleAppendPart3 = []byte(`","time":"`)
	handleAppendPart4 = []byte(`"}`)
)

func writeAppendResponse(
	ctx *fasthttp.RequestCtx,
	buf *bufpool.Buffer,
	offset,
	newVersion uint64,
	tm time.Time,
) error {
	_, _ = ctx.Write(handleAppendPart1)
	// Write offset
	_, _ = hex.WriteUint64(ctx, offset)
	_, _ = ctx.Write(handleAppendPart2)
	// Write new version
	_, _ = hex.WriteUint64(ctx, newVersion)
	_, _ = ctx.Write(handleAppendPart3)
	// Write time
	_, _ = ctx.WriteString(tm.Format(time.RFC3339))
	_, _ = ctx.Write(handleAppendPart4)

	return nil
}
