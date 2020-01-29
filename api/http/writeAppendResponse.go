package http

import (
	"fmt"
	"time"

	"github.com/romshark/eventlog/internal/bufpool"
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
	if err := writeBase64Uint64(offset, buf, ctx); err != nil {
		return fmt.Errorf("encoding offset base64: %w", err)
	}
	_, _ = ctx.Write(handleAppendPart2)
	// Write new version
	if err := writeBase64Uint64(newVersion, buf, ctx); err != nil {
		return fmt.Errorf("encoding newVersion base64: %w", err)
	}
	_, _ = ctx.Write(handleAppendPart3)
	// Write time
	_, _ = ctx.WriteString(tm.Format(time.RFC3339))
	_, _ = ctx.Write(handleAppendPart4)

	return nil
}
