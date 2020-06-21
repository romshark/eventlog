package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/romshark/eventlog/internal/consts"

	"github.com/valyala/fasthttp"
)

const (
	methodGet  = "GET"
	methodPost = "POST"
	pathLog    = "log/"
	pathBegin  = "begin"
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
	payload map[string]interface{},
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if len(payload) < 1 {
		err = ErrInvalidPayload
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		err = fmt.Errorf("marshaling event body: %w", err)
		return
	}

	return c.appendBytes(true, assumedVersion, body)
}

// AppendCheckMulti implements Client.AppendCheckMulti
func (c *HTTP) AppendCheckMulti(
	assumedVersion string,
	payload ...map[string]interface{},
) (
	offset string,
	newVersion string,
	tm time.Time,
	err error,
) {
	if len(payload) < 1 {
		err = ErrInvalidPayload
		return
	}

	body, err := json.Marshal(payload)
	if err != nil {
		err = fmt.Errorf("marshaling event body: %w", err)
		return
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
