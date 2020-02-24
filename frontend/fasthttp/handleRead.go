package fasthttp

import (
	"errors"
	"time"

	eventlog "github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"
	"github.com/romshark/eventlog/internal/hex"
	"github.com/romshark/eventlog/internal/itoa"

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
func (s *Server) handleRead(ctx *fasthttp.RequestCtx) error {
	buf := make([]byte, 0, 64)

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

	counter := uint32(0)

	_, _ = ctx.Write(partH1)
	firstCall := true
	err = s.eventLog.Scan(
		offset,
		n,
		func(timestamp uint64, payload []byte, offset uint64) error {
			if !firstCall {
				_, _ = ctx.Write(partSeparator)
			}
			firstCall = false

			counter++
			_, _ = ctx.Write(partE1)

			buf = time.Unix(int64(timestamp), 0).AppendFormat(buf, time.RFC3339)
			_, _ = ctx.Write(buf)
			buf = buf[:0]

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
	_ = itoa.U32toa(ctx, counter)
	_, _ = ctx.Write(partCloseBlock)

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}
