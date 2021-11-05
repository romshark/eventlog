package fasthttp

import (
	"bytes"
	"errors"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal"
	"github.com/romshark/eventlog/internal/hex"
	"github.com/romshark/eventlog/internal/msgcodec"

	"github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
)

var (
	methodGet          = []byte("GET")
	methodPost         = []byte("POST")
	pathLog            = []byte("/log/")
	pathMeta           = []byte("/meta")
	pathVersionInitial = []byte("/version/initial")
	pathVersion        = []byte("/version")
	pathSubscription   = []byte("/subscription")
)

// Serve handles incomming requests from a fasthttp.Server
func (s *Server) Serve(ctx *fasthttp.RequestCtx) {
	m := ctx.Method()
	p := ctx.Path()

	var handle func(ctx *fasthttp.RequestCtx) error

	switch {
	case bytes.Equal(m, methodGet):
		switch {
		case len(p) > len(pathLog) &&
			bytes.HasPrefix(p, pathLog):
			// GET /log/:version
			handle = s.handleScan
		case bytes.Equal(p, pathVersionInitial):
			// GET /version/initial
			handle = s.handleVersionInitial
		case bytes.Equal(p, pathVersion):
			// GET /version
			handle = s.handleVersion
		case bytes.Equal(p, pathSubscription):
			// GET /subscription
			handle = s.handleSubscription
		case bytes.Equal(p, pathMeta):
			// GET /meta
			handle = s.handleMeta
		}
	case bytes.Equal(m, methodPost):
		switch {
		case bytes.Equal(p, pathLog):
			// POST /log/
			handle = s.handleAppendNocheck
		case len(p) > len(pathLog) &&
			bytes.HasPrefix(p, pathLog):
			// POST /log/:assumedVersion
			handle = s.handleAppendCheck
		}
	default:
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusMethodNotAllowed),
			fasthttp.StatusMethodNotAllowed,
		)
		return
	}

	if handle == nil {
		// No handler selected
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusNotFound),
			fasthttp.StatusNotFound,
		)
		return
	}

	// Handle request
	if err := handle(ctx); err != nil {
		ctx.Error(
			fasthttp.StatusMessage(fasthttp.StatusInternalServerError),
			fasthttp.StatusInternalServerError,
		)
		log.Print("error: ", err)
		return
	}
}

// handleAppendCheck handles POST /log/:assumedVersion
func (s *Server) handleAppendCheck(ctx *fasthttp.RequestCtx) error {
	assumedVersion, err := hex.ReadUint64(ctx.Path()[len(pathLog):])
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(internal.StatusMsgErrInvalidVersion)
		return nil
	}

	if err := handleAppend(
		ctx,
		func(e eventlog.EventData) (
			versionPrevious uint64,
			version uint64,
			tm time.Time,
			err error,
		) {
			version, tm, err = s.eventLog.AppendCheck(assumedVersion, e)
			versionPrevious = assumedVersion
			return
		},
		func(e ...eventlog.EventData) (
			versionPrevious uint64,
			versionFirst uint64,
			version uint64,
			tm time.Time,
			err error,
		) {
			versionFirst, version, tm, err =
				s.eventLog.AppendCheckMulti(assumedVersion, e...)
			versionPrevious = assumedVersion
			return
		},
	); err == eventlog.ErrMismatchingVersions {
		ctx.SetBodyString(internal.StatusMsgErrMismatchingVersions)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return nil
	}
	return nil
}

// handleAppendNocheck handles POST /log/
func (s *Server) handleAppendNocheck(ctx *fasthttp.RequestCtx) error {
	return handleAppend(ctx, s.eventLog.Append, s.eventLog.AppendMulti)
}

// handleMeta handles GET /meta
func (s *Server) handleMeta(ctx *fasthttp.RequestCtx) error {
	ln := s.eventLog.MetadataLen()
	if ln < 1 {
		_, _ = ctx.WriteString(`{}`)
	} else {
		_, _ = ctx.WriteString(`{"`)
		i := 0
		s.eventLog.ScanMetadata(func(field, value string) bool {
			_, _ = ctx.WriteString(field)
			_, _ = ctx.WriteString(`":"`)
			_, _ = ctx.WriteString(value)
			i++
			if i < ln {
				// Not last
				_, _ = ctx.WriteString(`","`)
			}
			return true
		})
		_, _ = ctx.WriteString(`"}`)
	}

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}

// handleVersionInitial handles GET /version-initial
func (s *Server) handleVersionInitial(ctx *fasthttp.RequestCtx) error {
	v := s.eventLog.VersionInitial()
	_, _ = ctx.WriteString(`{"version-initial":"`)
	_, _ = hex.WriteUint64(ctx, v)
	_, _ = ctx.WriteString(`"}`)
	return nil
}

// handleScan handles GET /log/:version
func (s *Server) handleScan(ctx *fasthttp.RequestCtx) error {
	buf := make([]byte, 0, 64)

	version, err := hex.ReadUint64(ctx.Path()[len(pathLog):])
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(internal.StatusMsgErrMalformedVersion)
		return nil
	}

	var n int
	if a := ctx.QueryArgs().Peek("n"); a != nil {
		var err error
		n, err = strconv.Atoi(string(a))
		if err != nil {
			ctx.ResetBody()
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.SetBodyString(internal.StatusMsgErrBadArgument)
			return nil
		}
	}
	n = AdjustBatchSize(n, s.maxReadBatchSize)

	reverse := false
	if a := ctx.QueryArgs().Peek("reverse"); a != nil {
		if string(a) != "true" {
			ctx.ResetBody()
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			ctx.SetBodyString(internal.StatusMsgErrBadArgument)
			return nil
		}
		reverse = true
	}

	_, _ = ctx.WriteString(`[`)

	i := 0
	if err := s.eventLog.Scan(
		version,
		reverse,
		func(e eventlog.Event) error {
			_, _ = ctx.WriteString(`{"time":"`)
			buf = time.Unix(int64(e.Timestamp), 0).
				AppendFormat(buf, time.RFC3339)
			_, _ = ctx.Write(buf)
			buf = buf[:0]

			_, _ = ctx.WriteString(`","version":"`)
			_, _ = hex.WriteUint64(ctx, e.Version)

			_, _ = ctx.WriteString(`","version-previous":"`)
			_, _ = hex.WriteUint64(ctx, e.VersionPrevious)

			_, _ = ctx.WriteString(`","version-next":"`)
			_, _ = hex.WriteUint64(ctx, e.VersionNext)

			_, _ = ctx.WriteString(`","label":"`)
			_, _ = ctx.Write(e.Label)

			_, _ = ctx.WriteString(`","payload":`)
			_, _ = ctx.Write(e.PayloadJSON)
			_, _ = ctx.WriteString(`}`)

			i++
			if reverse && e.VersionPrevious == 0 ||
				!reverse && e.VersionNext == 0 ||
				n > 0 && i >= n {
				return errAbortScan
			}
			_, _ = ctx.WriteString(`,`)
			return nil
		},
	); err == errAbortScan {
		// Ignore
	} else if errors.Is(err, eventlog.ErrInvalidVersion) {
		ctx.ResetBody()
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(internal.StatusMsgErrInvalidVersion)
		return nil
	} else if err != nil {
		return err
	}

	_, _ = ctx.WriteString(`]`)

	ctx.Response.SetStatusCode(fasthttp.StatusOK)
	ctx.Response.Header.SetContentType("application/json")

	return nil
}

var errAbortScan = errors.New("as")

// handleAppend handles both append and check-append requests
func handleAppend(
	ctx *fasthttp.RequestCtx,
	appendSingle func(event eventlog.EventData) (
		versionPrevious uint64,
		version uint64,
		tm time.Time,
		err error,
	),
	appendMulti func(events ...eventlog.EventData) (
		versionPrevious uint64,
		versionFirst uint64,
		version uint64,
		tm time.Time,
		err error,
	),
) (err error) {
	var single eventlog.EventData
	var events []eventlog.EventData
	num := 0

	var (
		version         uint64
		versionPrevious uint64
		versionFirst    uint64
		tm              time.Time
	)

	switch err = msgcodec.ScanBytesBinary(
		ctx.Request.Body(),
		func(n int) error {
			num = n
			if n > 1 {
				events = make([]eventlog.EventData, 0, n)
			}
			return nil
		},
		func(label []byte, payloadJSON []byte) error {
			if num < 2 {
				// Single event
				single.Label = label
				single.PayloadJSON = payloadJSON
				return errSingleEvent
			}
			// Multiple events
			events = append(events, eventlog.EventData{
				Label:       label,
				PayloadJSON: payloadJSON,
			})
			return nil
		},
	); err {
	case msgcodec.ErrMalformedMessage:
		ctx.SetBodyString(internal.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		err = nil
		return

	case eventlog.ErrInvalidPayload:
		ctx.SetBodyString(internal.StatusMsgErrInvalidPayload)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		err = nil
		return

	case errSingleEvent:
		// Single event
		versionPrevious, version, tm, err = appendSingle(single)

		_, _ = ctx.WriteString(`{"version":"`)
		_, _ = hex.WriteUint64(ctx, version)
		_, _ = ctx.WriteString(`","version-previous":"`)
		_, _ = hex.WriteUint64(ctx, versionPrevious)
		_, _ = ctx.WriteString(`","time":"`)
		_, _ = writeTime(ctx, tm)
		_, _ = ctx.WriteString(`"}`)

	case nil:
		// Multiple events
		versionPrevious, versionFirst, version, tm, err =
			appendMulti(events...)

		_, _ = ctx.WriteString(`{"version":"`)
		_, _ = hex.WriteUint64(ctx, version)
		_, _ = ctx.WriteString(`","version-previous":"`)
		_, _ = hex.WriteUint64(ctx, versionPrevious)
		_, _ = ctx.WriteString(`","version-first":"`)
		_, _ = hex.WriteUint64(ctx, versionFirst)
		_, _ = ctx.WriteString(`","time":"`)
		_, _ = writeTime(ctx, tm)
		_, _ = ctx.WriteString(`"}`)
	}
	return
}

func (s *Server) handleVersion(ctx *fasthttp.RequestCtx) error {
	v := s.eventLog.Version()
	_, _ = ctx.WriteString(`{"version":"`)
	_, _ = hex.WriteUint64(ctx, v)
	_, _ = ctx.WriteString(`"}`)
	return nil
}

func (s *Server) handleSubscription(ctx *fasthttp.RequestCtx) error {
	return s.wsUpgrader.Upgrade(ctx, func(conn *websocket.Conn) {
		ch, closeSub := s.eventLog.Subscribe()

		pingTicker := time.NewTicker(s.wsPingInterval)
		closed := uint32(0)

		// closeConn is expected to be called while s.lock is locked!
		closeConn := func() {
			if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
				// Already closed
				return
			}
			closeSub()
			pingTicker.Stop()
			conn.Close()

			delete(s.wsConns, conn)
		}
		write := func(msgType int, msg []byte) error {
			if err := conn.SetWriteDeadline(
				time.Now().Add(s.wsWriteTimeout),
			); err != nil {
				return err
			}
			return conn.WriteMessage(msgType, msg)
		}

		defer func() {
			s.lock.Lock()
			defer s.lock.Unlock()
			closeConn()
		}()

		s.lock.Lock()
		s.wsConns[conn] = closeConn
		s.lock.Unlock()

		go func() {
			for {
				select {
				case version, ok := <-ch:
					// Send version update notification
					if !ok {
						return
					}

					// Encode version to hex byte string
					msg := []byte(strconv.FormatUint(version, 16))

					if err := write(
						websocket.BinaryMessage,
						msg,
					); err != nil {
						return
					}
				case _, ok := <-pingTicker.C:
					// Send ping control message
					if !ok {
						return
					}

					if err := write(
						websocket.PingMessage,
						nil,
					); err != nil {
						return
					}
				}
			}
		}()

		// Setup listener
		conn.SetReadLimit(1)
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			s.logErr.Printf(
				"ERR: disabling websocket read timeout: %s\n",
				err,
			)
		}
		_, _, _ = conn.NextReader()
	})
}

var errSingleEvent = errors.New("se")

func writeTime(ctx *fasthttp.RequestCtx, tm time.Time) (int, error) {
	b := make([]byte, 0, 64)
	b = tm.AppendFormat(b, time.RFC3339)
	return ctx.Write(b)
}

func AdjustBatchSize(requested int, limit int) int {
	if limit == 0 {
		// There's no limit
		return requested
	}
	// There's a limit
	if requested == 0 || requested > limit {
		return limit
	}
	return requested
}
