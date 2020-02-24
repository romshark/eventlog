package fasthttp

import (
	"errors"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"
	"github.com/romshark/eventlog/internal/hex"

	"github.com/valyala/fasthttp"
)

// handleAppendCheck handles POST /log/:assumedVersion
func (s *Server) handleAppendCheck(ctx *fasthttp.RequestCtx) error {
	assumedVersion, err := hex.ReadUint64(ctx.Path()[len(uriLog):])
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody(consts.StatusMsgErrInvalidVersion)
		return nil
	}

	offset, newVersion, tm, err := s.eventLog.AppendCheck(
		assumedVersion,
		ctx.PostBody(),
	)
	switch {
	case errors.Is(err, eventlog.ErrMismatchingVersions):
		ctx.SetBody(consts.StatusMsgErrMismatchingVersions)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	case errors.Is(err, eventlog.ErrInvalidPayload):
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	case err != nil:
		return err
	}

	return writeAppendResponse(ctx, offset, newVersion, tm)
}
