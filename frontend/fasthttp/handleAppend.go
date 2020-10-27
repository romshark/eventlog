package fasthttp

import (
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"
	"github.com/romshark/eventlog/internal/msgcodec"
	"github.com/valyala/fasthttp"
)

// handleAppend handles both append and check-append requests
func handleAppend(
	ctx *fasthttp.RequestCtx,
	appendSingle func(eventlog.Event) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	),
	appendMulti func(...eventlog.Event) (
		offset uint64,
		newVersion uint64,
		tm time.Time,
		err error,
	),
) (err error) {
	var single eventlog.Event
	var events []eventlog.Event
	num := 0

	var (
		offset     uint64
		newVersion uint64
		tm         time.Time
	)

	switch err = msgcodec.ScanBytesBinary(
		ctx.Request.Body(),
		func(n int) error {
			num = n
			if n > 1 {
				events = make([]eventlog.Event, 0, n)
			}
			return nil
		},
		func(label []byte, payloadJSON []byte) error {
			if num < 2 {
				// Single event
				single.Label = string(label)
				single.PayloadJSON = payloadJSON
				return errSingleEvent
			} else {
				// Multiple events
				events = append(events, eventlog.Event{
					Label:       string(label),
					PayloadJSON: payloadJSON,
				})
			}
			return nil
		},
	); err {
	case errSingleEvent:
		// Single event
		offset, newVersion, tm, err = appendSingle(single)

	case nil:
		// Multiple events
		offset, newVersion, tm, err = appendMulti(events...)

	case msgcodec.ErrMalformedMessage:
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		err = nil
		return
	}

	if err == eventlog.ErrInvalidPayload {
		ctx.SetBody(consts.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		err = nil
		return
	}

	if err == nil {
		err = writeAppendResponse(ctx, offset, newVersion, tm)
	}
	return
}
