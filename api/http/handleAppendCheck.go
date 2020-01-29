package http

import (
	"errors"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

// handleAppendCheck handles POST /log/:assumedVersion
func (api *APIHTTP) handleAppendCheck(ctx *fasthttp.RequestCtx) error {
	buf := api.bufPool.Get()
	defer buf.Release()

	assumedVersion, ok := parseOffset(ctx, buf, ctx.Path()[len(uriLog):])
	if !ok {
		return nil
	}

	offset, newVersion, tm, err := api.eventLog.AppendCheck(
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

	return writeAppendResponse(ctx, buf, offset, newVersion, tm)
}
