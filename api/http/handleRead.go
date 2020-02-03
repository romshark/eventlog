package http

import (
	"errors"
	"strconv"
	"time"

	eventlog "github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"
	"github.com/romshark/eventlog/internal/hex"

	"github.com/valyala/fasthttp"
)

var (
	partH1         = []byte(`{"data":[`)
	partE1         = []byte(`{"time":"`)
	partE2         = []byte(`","offset":"`)
	partE3         = []byte(`","payload":`)
	partT1         = []byte(`],"len":`)
	partCloseBlock = []byte(`}`)
	partSeparator  = []byte(`,`)
)

// handleRead handles GET /log/:offset
func (api *APIHTTP) handleRead(ctx *fasthttp.RequestCtx) error {
	buf := api.bufPool.Get()
	defer buf.Release()

	offset, err := hex.ReadUint64(ctx.Path()[len(uriLog):])
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(consts.StatusMsgErrInvalidOffset)
		return nil
	}

	n, ok := parseQueryN(ctx)
	if !ok {
		return nil
	}

	counter := uint64(0)

	_, _ = ctx.Write(partH1)
	firstCall := true
	err = api.eventLog.Scan(
		offset,
		n,
		func(timestamp uint64, payload []byte, offset uint64) error {
			if !firstCall {
				_, _ = ctx.Write(partSeparator)
			}
			firstCall = false

			counter++
			_, _ = ctx.Write(partE1)
			_, _ = ctx.WriteString(
				time.Unix(int64(timestamp), 0).Format(time.RFC3339),
			)
			_, _ = ctx.Write(partE2)
			_, _ = hex.WriteUint64(ctx, offset)
			_, _ = ctx.Write(partE3)
			_, _ = ctx.Write(payload)
			_, _ = ctx.Write(partCloseBlock)

			return nil
		},
	)

	switch {
	case errors.Is(err, eventlog.ErrOffsetOutOfBound):
		ctx.ResetBody()
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(consts.StatusMsgErrOffsetOutOfBound)
		return nil
	case err != nil:
		return err
	}

	_, _ = ctx.Write(partT1)
	_, _ = ctx.WriteString(strconv.FormatUint(counter, 10))
	_, _ = ctx.Write(partCloseBlock)

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}
