package http

import (
	"errors"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

// handleAppend handles POST /log/:offset
func (api *APIHTTP) handleAppend(ctx *fasthttp.RequestCtx) error {
	err := api.eventLog.Append(ctx.PostBody())
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
