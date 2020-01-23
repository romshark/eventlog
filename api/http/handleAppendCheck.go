package http

import (
	"errors"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

// handleAppendCheck handles POST /log/:offset
func (api *APIHTTP) handleAppendCheck(ctx *fasthttp.RequestCtx) error {
	offset, ok := parseOffset(ctx, string(ctx.Path()[len(uriLog):]))
	if !ok {
		return nil
	}

	err := api.eventLog.AppendCheck(offset, ctx.PostBody())
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

	return nil
}
