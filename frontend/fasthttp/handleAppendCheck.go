package fasthttp

import (
	"encoding/json"
	"errors"
	"time"
	"unsafe"

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

	var (
		b          = ctx.Request.Body()
		offset     uint64
		newVersion uint64
		tm         time.Time
	)

	if len(b) < len(`{"x":0}`) {
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	}

	switch b[0] {
	case '{':
		// Single event
		offset, newVersion, tm, err = s.eventLog.AppendCheck(
			assumedVersion,
			b,
		)
	case '[':
		// Multiple events
		var a []json.RawMessage
		if err = json.Unmarshal(b, &a); err != nil {
			err = eventlog.ErrInvalidPayload
			break
		}
		offset, newVersion, tm, err = s.eventLog.AppendCheckMulti(
			assumedVersion,
			*(*[][]byte)(unsafe.Pointer(&a))...,
		)
	default:
		err = eventlog.ErrInvalidPayload
	}

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
