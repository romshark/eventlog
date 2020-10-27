package fasthttp

import (
	"errors"
	"time"

	eventlog "github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"
	"github.com/romshark/eventlog/internal/hex"

	"github.com/valyala/fasthttp"
)

var (
	partE1             = []byte(`{"time":"`)
	partE2             = []byte(`","offset":"`)
	partE3             = []byte(`","label":"`)
	partE4             = []byte(`","payload":`)
	partE5             = []byte(`,"next":"`)
	partCloseBlock     = []byte(`}`)
	partCloseBlockText = []byte(`"}`)
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

	nextOffset, err := s.eventLog.Scan(
		offset,
		1,
		func(
			offset uint64,
			timestamp uint64,
			label []byte,
			payloadJSON []byte,
		) error {
			_, _ = ctx.Write(partE1)
			buf = time.Unix(int64(timestamp), 0).AppendFormat(buf, time.RFC3339)
			_, _ = ctx.Write(buf)
			buf = buf[:0]

			_, _ = ctx.Write(partE2)
			_, _ = hex.WriteUint64(ctx, offset)

			_, _ = ctx.Write(partE3)
			_, _ = ctx.Write(label)

			_, _ = ctx.Write(partE4)
			_, _ = ctx.Write(payloadJSON)

			return nil
		},
	)

	switch {
	case errors.Is(err, eventlog.ErrOffsetOutOfBound):
		ctx.ResetBody()
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(consts.StatusMsgErrOffsetOutOfBound)
		return nil
	case errors.Is(err, eventlog.ErrInvalidOffset):
		ctx.ResetBody()
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(consts.StatusMsgErrInvalidOffset)
		return nil
	case err != nil:
		return err
	}

	if nextOffset == 0 {
		_, _ = ctx.Write(partCloseBlock)
	} else {
		_, _ = ctx.Write(partE5)
		_, _ = hex.WriteUint64(ctx, nextOffset)
		_, _ = ctx.Write(partCloseBlockText)
	}

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}
