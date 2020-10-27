package fasthttp

import (
	"errors"
	"time"

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

	if err := handleAppend(
		ctx,
		func(e eventlog.Event) (
			offset uint64,
			newVersion uint64,
			tm time.Time,
			err error,
		) {
			return s.eventLog.AppendCheck(assumedVersion, e)
		},
		func(e ...eventlog.Event) (
			offset uint64,
			newVersion uint64,
			tm time.Time,
			err error,
		) {
			return s.eventLog.AppendCheckMulti(assumedVersion, e...)
		},
	); errors.Is(err, eventlog.ErrMismatchingVersions) {
		ctx.SetBody(consts.StatusMsgErrMismatchingVersions)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	}
	return nil
}
