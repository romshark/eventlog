package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

const (
	methodGet  = "GET"
	methodPost = "POST"
	pathLog    = "log/"
	queryArgsN = "n"
)

var (
	errOutOfBound = []byte("ErrOutOfBound")
)

// Make sure *HTTP implements Client
var _ Client = new(HTTP)

// HTTP represents an HTTP eventlog client
type HTTP struct {
	clt  *fasthttp.Client
	host string
}

// NewHTTP creates a new HTTP eventlog client
func NewHTTP(clt *fasthttp.Client, host string) *HTTP {
	if clt == nil {
		clt = &fasthttp.Client{}
	}
	return &HTTP{
		clt:  clt,
		host: host,
	}
}

// Append implements Client.Append
func (c *HTTP) Append(payload map[string]interface{}) error {
	if len(payload) < 1 {
		return ErrInvalidPayload
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling event body: %w", err)
	}

	return c.appendBytes(false, 0, body, false)
}

// AppendCheck implements Client.AppendCheck
func (c *HTTP) AppendCheck(
	offset uint64,
	payload map[string]interface{},
) error {
	if len(payload) < 1 {
		return ErrInvalidPayload
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshaling event body: %w", err)
	}

	return c.appendBytes(true, offset, body, false)
}

// AppendBytes implements Client.AppendCheck
func (c *HTTP) AppendBytes(payload []byte) error {
	return c.appendBytes(false, 0, payload, true)
}

// AppendCheckBytes implements Client.AppendCheck
func (c *HTTP) AppendCheckBytes(
	offset uint64,
	payload []byte,
) error {
	return c.appendBytes(true, offset, payload, true)
}

func (c *HTTP) appendBytes(
	includeOffset bool,
	offset uint64,
	payload []byte,
	validateJSON bool,
) error {
	if len(payload) < 1 {
		return ErrInvalidPayload
	}
	if validateJSON {
		if err := eventlog.VerifyPayload(payload); err != nil {
			return err
		}
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodPost)

	if includeOffset {
		req.URI().SetPath(pathLog + strconv.FormatUint(offset, 10))
	} else {
		req.URI().SetPath(pathLog)
	}

	req.SetBody(payload)

	if err := c.clt.Do(req, resp); err != nil {
		return fmt.Errorf("http request: %w", err)
	}

	if resp.StatusCode() == fasthttp.StatusBadRequest {
		switch {
		case bytes.Equal(resp.Body(), consts.StatusMsgErrMismatchingVersions):
			return ErrMismatchingVersions
		case bytes.Equal(resp.Body(), consts.StatusMsgErrInvalidPayload):
			return ErrInvalidPayload
		}
		return fmt.Errorf(
			"unexpected client-side error: (%d) %s",
			resp.StatusCode(),
			string(resp.Body()),
		)
	} else if resp.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	return nil
}

// Read implements Client.Read
func (c *HTTP) Read(
	offset uint64,
	n uint64,
) ([]Event, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetHost(c.host)
	req.Header.SetMethod(methodGet)
	req.URI().SetPath(pathLog + strconv.FormatUint(offset, 10))

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
