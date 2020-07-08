package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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
	queryArgsN  = "n"
)

var (
	errOutOfBound = []byte("ErrOutOfBound")
)

// Make sure *HTTP implements Client
var _ Client = new(HTTP)

// HTTP represents an HTTP eventlog client
type HTTP struct {
	logErr   Log
	clt      *fasthttp.Client
	host     string
	wsDialer *websocket.Dialer
}

// NewHTTP creates a new HTTP eventlog client
func NewHTTP(
	logErr Log,
	clt *fasthttp.Client,
	wsDialer *websocket.Dialer,
	host string,
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
		logErr:   logErr,
		clt:      clt,
		wsDialer: wsDialer,
		host:     host,
	}
}

// Append implements Client.Append
func (c *HTTP) Append(payload ...map[string]interface{}) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	var body []byte
	switch l := len(payload); {
	case l < 1:
		err = ErrInvalidPayload
		return
	case l == 1:
		body, err = json.Marshal(payload[0])
		if err != nil {
			err = fmt.Errorf("marshaling event body: %w", err)
			return
		}
	case l > 1:
		body, err = json.Marshal(payload)
		if err != nil {
			err = fmt.Errorf("marshaling multiple event bodies: %w", err)
			return
		}
	}
	return c.appendBytes(false, "", body)
}

// AppendCheck implements Client.AppendCheck
func (c *HTTP) AppendCheck(
	assumedVersion string,
	payload ...map[string]interface{},
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if assumedVersion == "" {
		err = errors.New("no assumed version")
		return
	}

	var body []byte
	switch l := len(payload); {
	case l < 1:
		err = ErrInvalidPayload
		return
	case l == 1:
		body, err = json.Marshal(payload[0])
		if err != nil {
			err = fmt.Errorf("marshaling event body: %w", err)
			return
		}
	case l > 1:
		body, err = json.Marshal(payload)
		if err != nil {
			err = fmt.Errorf("marshaling multiple event bodies: %w", err)
			return
		}
	}
	return c.appendBytes(true, assumedVersion, body)
}

// AppendBytes implements Client.AppendCheck
func (c *HTTP) AppendBytes(payload []byte) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.appendBytes(false, "", payload)
}

// AppendCheckBytes implements Client.AppendCheckBytes
func (c *HTTP) AppendCheckBytes(
	assumedVersion string,
	payload []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	return c.appendBytes(true, assumedVersion, payload)
}

func (c *HTTP) appendBytes(
	assumeVersion bool,
	assumedVersion string,
	payloadJSON []byte,
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if len(payloadJSON) < 1 {
		err = ErrInvalidPayload
		return
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodPost)

	if assumeVersion {
		req.URI().SetPath(pathLog + assumedVersion)
	} else {
		req.URI().SetPath(pathLog)
	}

	req.SetBody(payloadJSON)

	if err = c.clt.Do(req, resp); err != nil {
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
func (c *HTTP) Read(
	offset string,
	n uint64,
) ([]Event, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathLog + offset)

	args := req.URI().QueryArgs()
	if n > 0 {
		args.Set(queryArgsN, strconv.FormatUint(n, 10))
	}

	if err := c.clt.Do(req, resp); err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}

	switch {
	case resp.StatusCode() == fasthttp.StatusBadRequest &&
		bytes.Equal(resp.Body(), errOutOfBound):
	}

	if resp.StatusCode() == fasthttp.StatusBadRequest {
		switch {
		case bytes.Equal(resp.Body(), consts.StatusMsgErrOffsetOutOfBound):
			return nil, ErrOffsetOutOfBound
		}
		return nil, fmt.Errorf(
			"unexpected client-side error: (%d) %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	} else if resp.StatusCode() != fasthttp.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status code: %d",
			resp.StatusCode(),
		)
	}

	var events struct {
		Len  uint    `json:"len"`
		Data []Event `json:"data"`
	}

	if err := json.Unmarshal(resp.Body(), &events); err != nil {
		return nil, fmt.Errorf("unmarshalling response body: %w", err)
	}

	return events.Data, nil
}

// Begin implements Client.Begin
func (c *HTTP) Begin() (string, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathBegin)

	if err := c.clt.Do(req, resp); err != nil {
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

func (c *HTTP) Version() (string, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathVersion)

	if err := c.clt.Do(req, resp); err != nil {
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

type Log interface {
	Printf(format string, v ...interface{})
}
