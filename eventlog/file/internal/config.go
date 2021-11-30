package internal

import (
	"github.com/romshark/eventlog/eventlog"
)

type Config struct{ MinPayloadLen, MaxPayloadLen int }

func (c Config) VerifyPayloadLen(p []byte) error {
	if l := len(p); l > c.MaxPayloadLen {
		return eventlog.ErrPayloadSizeLimitExceeded
	}
	return nil
}
