package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/romshark/eventlog/internal/consts"

	"github.com/fasthttp/websocket"
	"github.com/valyala/fasthttp"
)

const (
	methodGet   = "GET"
	methodPost  = "POST"
	pathLog     = "log/"
	pathVersion = "version"
	pathBegin   = "begin"
)

var (
	errOutOfBound = []byte("ErrOutOfBound")
)

// Make sure *HTTP implements Client
var _ Implementer = new(HTTP)

// HTTP represents an HTTP eventlog client
type HTTP struct {
	host     string
	logErr   Log
	clt      *fasthttp.Client
	wsDialer *websocket.Dialer
}

// NewHTTP creates a new HTTP eventlog client
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

func (c *HTTP) Append(
	ctx context.Context,
	assumeVersion bool,
	assumedVersion string,
	eventsEncoded []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodPost)
	req.Header.SetContentType("application/octet-stream")

	if assumeVersion {
		req.URI().SetPath(pathLog + assumedVersion)
	} else {
		req.URI().SetPath(pathLog)
	}

	req.SetBody(eventsEncoded)

	if d, ok := ctx.Deadline(); ok {
		err = c.clt.DoDeadline(req, resp, d)
	} else {
		err = c.clt.Do(req, resp)
	}
	if err != nil {
		err = fmt.Errorf("http request: %w", err)
		return
	}

	if resp.StatusCode() == fasthttp.StatusBadRequest {
		switch {
		case bytes.Equal(resp.Body(), consts.StatusMsgErrMismatchingVersions):
			err = ErrMismatchingVersions
			return
		case bytes.Equal(resp.Body(), consts.StatusMsgErrInvalidPayload):
			err = ErrInvalidPayload
			return
		}
		err = fmt.Errorf(
			"unexpected client-side error: (%d) %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
		return
	} else if resp.StatusCode() != fasthttp.StatusOK {
		err = fmt.Errorf("unexpected status code: %d", resp.StatusCode())
		return
	}

	var re struct {
		Offset     string    `json:"offset"`
		NewVersion string    `json:"newVersion"`
		Time       time.Time `json:"time"`
	}
	if err = json.Unmarshal(resp.Body(), &re); err != nil {
		err = fmt.Errorf("unmarshalling response: %w", err)
		return
	}

	return re.Offset, re.NewVersion, re.Time, nil
}

// Read implements Client.Read
//
// WARNING: manually cancelable (non-timeout and non-deadline) contexts
// are not supported.
func (c *HTTP) Read(
	ctx context.Context,
	offset string,
) (Event, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathLog + offset)

	var err error
	if d, ok := ctx.Deadline(); ok {
		err = c.clt.DoDeadline(req, resp, d)
	} else {
		err = c.clt.Do(req, resp)
	}
	if err != nil {
		return Event{}, fmt.Errorf("http request: %w", err)
	}

	switch {
	case resp.StatusCode() == fasthttp.StatusBadRequest &&
		bytes.Equal(resp.Body(), errOutOfBound):
	}

	if resp.StatusCode() == fasthttp.StatusBadRequest {
		switch {
		case bytes.Equal(resp.Body(), consts.StatusMsgErrOffsetOutOfBound):
			return Event{}, ErrOffsetOutOfBound
		}
		return Event{}, fmt.Errorf(
			"unexpected client-side error: (%d) %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	} else if resp.StatusCode() != fasthttp.StatusOK {
		return Event{}, fmt.Errorf(
			"unexpected status code: %d",
			resp.StatusCode(),
		)
	}

	var e struct {
		Offset  string          `json:"offset"`
		Time    time.Time       `json:"time"`
		Next    string          `json:"next"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(resp.Body(), &e); err != nil {
		return Event{}, fmt.Errorf("unmarshalling response body: %w", err)
	}

	return Event{
		Offset:  e.Offset,
		Time:    e.Time,
		Next:    e.Next,
		Payload: e.Payload,
	}, nil
}

// Begin implements Client.Begin
func (c *HTTP) Begin(ctx context.Context) (string, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathBegin)

	var err error
	if d, ok := ctx.Deadline(); ok {
		err = c.clt.DoDeadline(req, resp, d)
	} else {
		err = c.clt.Do(req, resp)
	}
	if err != nil {
		return "", fmt.Errorf("http request: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return "", fmt.Errorf(
			"unexpected status code: %d",
			resp.StatusCode(),
		)
	}

	b := resp.Body()
	if len(b) < 14 {
		return "", fmt.Errorf(
			"unexpected response body: %s",
			string(b),
		)
	}

	return string(b[11 : len(b)-2]), nil
}

func (c *HTTP) Version(ctx context.Context) (string, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathVersion)

	var err error
	if d, ok := ctx.Deadline(); ok {
		err = c.clt.DoDeadline(req, resp, d)
	} else {
		err = c.clt.Do(req, resp)
	}
	if err != nil {
		return "", fmt.Errorf("http request: %w", err)
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return "", fmt.Errorf(
			"unexpected status code: %d",
			resp.StatusCode(),
		)
	}

	b := resp.Body()
	if len(b) < 14 {
		return "", fmt.Errorf(
			"unexpected response body: %s",
			string(b),
		)
	}

	return string(b[12 : len(b)-2]), nil
}

// Listen establishes a websocket connection to the server
// and starts listening for version update notifications
// calling onUpdate when one is received.
func (c *HTTP) Listen(ctx context.Context, onUpdate func([]byte)) error {
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
		onUpdate(buf[:n])
	}
}

var ErrSocketClosed = errors.New("socket closed")
