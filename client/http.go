package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	intrn "github.com/romshark/eventlog/internal"
	"github.com/romshark/eventlog/internal/msgcodec"

	"github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
)

const (
	methodGet          = "GET"
	methodPost         = "POST"
	pathLog            = "log/"
	pathMeta           = "meta"
	pathVersion        = "version"
	pathVersionInitial = "version/initial"
)

// Make sure *HTTP implements the ReadWriter
var _ ReadWriter = new(HTTP)

// HTTP is an HTTP eventlog read-writer.
type HTTP struct {
	host          string
	logErr        Log
	clt           *fasthttp.Client
	wsDialer      *websocket.Dialer
	retryInterval time.Duration
}

// NewHTTP creates a read-writer connecting to an eventlog's HTTP API.
func NewHTTP(
	host string,
	logErr Log,
	clt *fasthttp.Client,
	wsDialer *websocket.Dialer,
) *HTTP {
	if clt == nil {
		clt = &fasthttp.Client{}
	}
	if wsDialer == nil {
		wsDialer = &websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 45 * time.Second,
			ReadBufferSize:   16,
		}
	}
	return &HTTP{
		host:     host,
		logErr:   logErr,
		clt:      clt,
		wsDialer: wsDialer,
	}
}

// GetRetryInterval returns the current retry interval.
// Returns 0 if automatic retry is disabled.
func (c *HTTP) GetRetryInterval() time.Duration {
	return time.Duration(atomic.LoadInt64((*int64)(&c.retryInterval)))
}

// SetRetryInterval enables automatic retries if d > 0,
// otherwise automatic retry is disabled.
func (c *HTTP) SetRetryInterval(d time.Duration) {
	atomic.StoreInt64((*int64)(&c.retryInterval), int64(d))
}

// Metadata implements Reader.Metadata.
func (c *HTTP) Metadata(ctx context.Context) (map[string]string, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathMeta)

	var err error
	if d, ok := ctx.Deadline(); ok {
		err = c.clt.DoDeadline(req, resp, d)
	} else {
		err = c.clt.Do(req, resp)
	}
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status code: %d (%q)",
			resp.StatusCode(),
			string(resp.Body()),
		)
	}

	var m map[string]string
	if err := json.Unmarshal(resp.Body(), &m); err != nil {
		return nil, fmt.Errorf("unmarshalling response body: %w", err)
	}

	return m, nil
}

func (c *HTTP) req(
	ctx context.Context,
	prepare func(*fasthttp.Request),
	handle func(*fasthttp.Response) error,
) (err error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	prepare(req)

RETRY:
	for {
		if d, ok := ctx.Deadline(); ok {
			err = c.clt.DoDeadline(req, resp, d)
		} else {
			err = c.clt.Do(req, resp)
		}
		if err == nil {
			break RETRY
		}

		if ri := c.GetRetryInterval(); ri > 0 && isNetErrRecoverable(err) {
			// Retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(ri):
				continue RETRY
			}
		}
		return fmt.Errorf("http request: %w", err)
	}
	return handle(resp)
}

// Append implements Writer.Append.
func (c *HTTP) Append(
	ctx context.Context,
	event EventData,
) (
	versionPrevious Version,
	version Version,
	tm time.Time,
	err error,
) {
	var encoded []byte
	if encoded, err = msgcodec.EncodeBinary(event); err != nil {
		return
	}
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodPost)
			r.Header.SetContentType("application/octet-stream")
			r.URI().SetPath(pathLog)
			r.SetBody(encoded)
		},
		func(r *fasthttp.Response) error {
			if err := handleAppendErr(r); err != nil {
				return err
			}

			var re struct {
				VersionPrevious Version   `json:"version-previous"`
				Version         Version   `json:"version"`
				Time            time.Time `json:"time"`
			}
			if err = json.Unmarshal(r.Body(), &re); err != nil {
				return fmt.Errorf("unmarshalling response: %w", err)
			}

			versionPrevious = re.VersionPrevious
			version = re.Version
			tm = re.Time
			return nil
		},
	)
	return
}

// AppendMulti implements Writer.AppendMulti.
func (c *HTTP) AppendMulti(
	ctx context.Context,
	events ...EventData,
) (
	versionPrevious Version,
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	var encoded []byte
	if encoded, err = msgcodec.EncodeBinary(events...); err != nil {
		return
	}
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodPost)
			r.Header.SetContentType("application/octet-stream")
			r.URI().SetPath(pathLog)
			r.SetBody(encoded)
		},
		func(r *fasthttp.Response) error {
			if err := handleAppendErr(r); err != nil {
				return err
			}

			var re struct {
				VersionPrevious Version   `json:"version-previous"`
				VersionFirst    Version   `json:"version-first"`
				Version         Version   `json:"version"`
				Time            time.Time `json:"time"`
			}
			if err = json.Unmarshal(r.Body(), &re); err != nil {
				return fmt.Errorf("unmarshalling response: %w", err)
			}

			version = re.Version
			versionPrevious = re.VersionPrevious
			versionFirst = re.VersionFirst
			tm = re.Time
			return nil
		},
	)
	return
}

// AppendCheck implements Writer.AppendCheck.
func (c *HTTP) AppendCheck(
	ctx context.Context,
	assumedVersion Version,
	event EventData,
) (
	version Version,
	tm time.Time,
	err error,
) {
	var encoded []byte
	if encoded, err = msgcodec.EncodeBinary(event); err != nil {
		return
	}
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodPost)
			r.Header.SetContentType("application/octet-stream")
			r.URI().SetPath(pathLog + assumedVersion)
			r.SetBody(encoded)
		},
		func(r *fasthttp.Response) error {
			if err := handleAppendErr(r); err != nil {
				return err
			}

			var re struct {
				Version Version   `json:"version"`
				Time    time.Time `json:"time"`
			}
			if err = json.Unmarshal(r.Body(), &re); err != nil {
				return fmt.Errorf("unmarshalling response: %w", err)
			}

			version = re.Version
			tm = re.Time
			return nil
		},
	)
	return
}

// AppendCheckMulti implements Writer.AppendCheckMulti.
func (c *HTTP) AppendCheckMulti(
	ctx context.Context,
	assumedVersion Version,
	events ...EventData,
) (
	versionFirst Version,
	version Version,
	tm time.Time,
	err error,
) {
	var encoded []byte
	if encoded, err = msgcodec.EncodeBinary(events...); err != nil {
		return
	}
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodPost)
			r.Header.SetContentType("application/octet-stream")
			r.URI().SetPath(pathLog + assumedVersion)
			r.SetBody(encoded)
		},
		func(r *fasthttp.Response) error {
			if err := handleAppendErr(r); err != nil {
				return err
			}

			var re struct {
				VersionFirst Version   `json:"version-first"`
				Version      Version   `json:"version"`
				Time         time.Time `json:"time"`
			}
			if err = json.Unmarshal(r.Body(), &re); err != nil {
				return fmt.Errorf("unmarshalling response: %w", err)
			}

			version = re.Version
			versionFirst = re.VersionFirst
			tm = re.Time
			return nil
		},
	)
	return
}

// Scan implements Reader.Scan.
//
// WARNING: manually cancelable (non-timeout and non-deadline) contexts
// are not supported.
func (c *HTTP) Scan(
	ctx context.Context,
	version Version,
	reverse bool,
	skipFirst bool,
	fn func(Event) error,
) error {
SCAN:
	for {
		if err := c.req(
			ctx,
			func(r *fasthttp.Request) {
				r.Header.SetMethod(methodGet)
				r.URI().SetPath(pathLog + version)
				if reverse {
					r.URI().QueryArgs().SetNoValue("reverse")
				}
				if skipFirst {
					r.URI().QueryArgs().SetNoValue("skip_first")
				}
			},
			func(r *fasthttp.Response) error {
				if r.StatusCode() == fasthttp.StatusBadRequest {
					switch {
					case string(r.Body()) == intrn.StatusMsgErrMalformedVersion:
						return ErrMalformedVersion
					case string(r.Body()) == intrn.StatusMsgErrInvalidVersion:
						return ErrInvalidVersion
					}
					return fmt.Errorf(
						"unexpected client-side error: (%d) %s",
						r.StatusCode(),
						string(r.Body()),
					)
				} else if r.StatusCode() != fasthttp.StatusOK {
					return fmt.Errorf(
						"unexpected status code: %d (%q)",
						r.StatusCode(),
						string(r.Body()),
					)
				}

				var l []struct {
					Version         Version         `json:"version"`
					Time            time.Time       `json:"time"`
					VersionPrevious Version         `json:"version-previous"`
					VersionNext     Version         `json:"version-next"`
					Label           json.RawMessage `json:"label"`
					Payload         json.RawMessage `json:"payload"`
				}
				if err := json.Unmarshal(r.Body(), &l); err != nil {
					return fmt.Errorf("unmarshalling response body: %w", err)
				}

				for _, e := range l {
					if err := fn(Event{
						Version:         e.Version,
						Time:            e.Time.UTC(),
						VersionPrevious: e.VersionPrevious,
						VersionNext:     e.VersionNext,
						EventData: EventData{
							Label:       e.Label[1 : len(e.Label)-1],
							PayloadJSON: e.Payload,
						},
					}); err != nil {
						return err
					}
					if !reverse && e.VersionNext == "0" ||
						reverse && e.VersionPrevious == "0" {
						return errScanStopped
					}
				}

				if reverse {
					version = l[len(l)-1].VersionPrevious
				} else {
					version = l[len(l)-1].VersionNext
				}
				return nil
			},
		); err == errScanStopped {
			break SCAN
		} else if err != nil {
			return err
		}
	}
	return nil
}

var errScanStopped = errors.New("scan stopped")

// VersionInitial implements Reader.VersionInitial.
func (c *HTTP) VersionInitial(
	ctx context.Context,
) (version Version, err error) {
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodGet)
			r.URI().SetPath(pathVersionInitial)
		},
		func(r *fasthttp.Response) error {
			if r.StatusCode() != fasthttp.StatusOK {
				return fmt.Errorf(
					"unexpected status code: %d (%q)",
					r.StatusCode(),
					string(r.Body()),
				)
			}
			b := r.Body()
			if len(b) < 14 {
				return fmt.Errorf(
					"unexpected response body: %s",
					string(b),
				)
			}

			var re struct {
				VersionInitial Version `json:"version-initial"`
			}
			if err = json.Unmarshal(r.Body(), &re); err != nil {
				return fmt.Errorf("unmarshalling response: %w", err)
			}
			version = re.VersionInitial
			return nil
		},
	)
	return
}

// Version implements Reader.Version.
func (c *HTTP) Version(ctx context.Context) (version Version, err error) {
	err = c.req(
		ctx,
		func(r *fasthttp.Request) {
			r.Header.SetMethod(methodGet)
			r.URI().SetPath(pathVersion)
		},
		func(r *fasthttp.Response) error {
			if r.StatusCode() != fasthttp.StatusOK {
				return fmt.Errorf(
					"unexpected status code: %d (%q)",
					r.StatusCode(),
					string(r.Body()),
				)
			}

			b := r.Body()
			if len(b) < 14 {
				return fmt.Errorf(
					"unexpected response body: %s",
					string(b),
				)
			}

			version = string(b[12 : len(b)-2])
			return nil
		},
	)
	return
}

// Listen establishes a websocket connection to the server
// and starts listening for version update notifications
// calling onUpdate when one is received.
func (c *HTTP) Listen(ctx context.Context, onUpdate func(Version)) error {
	for {
		err := c.listen(ctx, onUpdate)
		if ri := c.GetRetryInterval(); ri > 0 &&
			(isNetErrRecoverable(err) || err == ErrSocketClosed) {
			// Retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(ri):
				continue
			}
		}
		return err
	}
}

func (c *HTTP) listen(ctx context.Context, onUpdate func(Version)) error {
	u := url.URL{
		Scheme: "ws",
		Host:   c.host,
		Path:   "/subscription",
	}

	conn, _, err := c.wsDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return err
	}
	closed := uint32(0)
	closeConn := func() {
		if !atomic.CompareAndSwapUint32(&closed, 0, 1) {
			// Already closed
			return
		}
		if err := conn.Close(); err != nil {
			c.logErr.Printf("ERR: closing socket: %s\n", err)
		}
	}
	defer closeConn()

	if ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			closeConn()
		}()
	}

	buf := make([]byte, 16)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		conn.SetReadLimit(16)
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			return fmt.Errorf("disabling read timeout on websocket: %w", err)
		}

		_, r, err := conn.NextReader()
		if err != nil {
			if !websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				return ErrSocketClosed
			}
			return err
		}
		n, err := r.Read(buf)
		switch {
		case err != nil:
			return fmt.Errorf("reading websocket: %w", err)
		case n > 16:
			return fmt.Errorf("excessive message length (%d/16)", n)
		}
		onUpdate(Version(buf[:n]))
	}
}

var ErrSocketClosed = errors.New("socket closed")

func isNetErrRecoverable(err error) bool {
	switch v := err.(type) {
	case *net.OpError:
		// Connection refused
		return v.Op == "dial" || v.Op == "read"
	case net.Error:
		return v.Temporary() || v.Timeout()
	}
	return false
}

func handleAppendErr(r *fasthttp.Response) error {
	if r.StatusCode() == fasthttp.StatusBadRequest {
		switch {
		case string(r.Body()) == intrn.StatusMsgErrMismatchingVersions:
			return ErrMismatchingVersions
		case string(r.Body()) == intrn.StatusMsgErrInvalidPayload:
			return ErrInvalidPayload
		case string(r.Body()) == intrn.StatusMsgErrPayloadSizeLimitExceeded:
			return ErrPayloadSizeLimitExceeded
		}
		return fmt.Errorf(
			"unexpected client-side error: (%d) %s",
			r.StatusCode(),
			string(r.Body()),
		)
	} else if r.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf(
			"unexpected status code: %d (%q)",
			r.StatusCode(),
			string(r.Body()),
		)
	}
	return nil
}
