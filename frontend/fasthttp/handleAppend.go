package fasthttp

import (
	"encoding/json"
	"errors"
	"time"
	"unsafe"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

// handleAppend handles POST /log/:offset
func (s *Server) handleAppend(ctx *fasthttp.RequestCtx) error {
	var (
		b          = ctx.Request.Body()
		offset     uint64
		newVersion uint64
		tm         time.Time
		err        error
	)

	if len(b) < len(`{"x":0}`) {
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	}

	switch b[0] {
	case '{':
		// Single event
		offset, newVersion, tm, err = s.eventLog.Append(b)
	case '[':
		// Multiple events
		var a []json.RawMessage
		if err = json.Unmarshal(b, &a); err != nil {
			err = eventlog.ErrInvalidPayload
			break
		}
		offset, newVersion, tm, err = s.eventLog.AppendMulti(
			*(*[][]byte)(unsafe.Pointer(&a))...,
		)
	default:
		err = eventlog.ErrInvalidPayload
	}

	switch {
	case errors.Is(err, eventlog.ErrInvalidPayload):
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	case err != nil:
		return err
	}

	return writeAppendResponse(ctx, offset, newVersion, tm)
}
