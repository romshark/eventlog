package eventlog

import (
	"errors"

	"github.com/valyala/fastjson"
)

// ValidatePayloadJSON returns an error if the given JSON payload is invalid
func ValidatePayloadJSON(payload []byte) error {
	if len(payload) < 1 {
		return ErrInvalidPayload
	}

	i := 0
	inObject := false
CHECK_INIT:
	for ; i < len(payload); i++ {
		switch payload[i] {
		case 0x20:
			// Space
		case 0x09:
			// Horizontal tab
		case 0x0A:
			// Line feed
		case 0x0D:
			// Carriage return
		case 0x7B:
			// Object open
			inObject = true
			break CHECK_INIT
		default:
			// The document doesn't begin with a '{'
			return ErrInvalidPayload
		}
	}

	if !inObject {
		return ErrInvalidPayload
	}

CHECK_INOBJ:
	for i++; i < len(payload); i++ {
		switch payload[i] {
		case 0x20:
			// Space
		case 0x09:
			// Horizontal tab
		case 0x0A:
			// Line feed
		case 0x0D:
			// Carriage return
		case 0x7D:
			// Object close
			// The '{' is directly followed by a '}', the object's empty
			return ErrInvalidPayload
		default:
			break CHECK_INOBJ
		}
	}

	if err := fastjson.ValidateBytes(payload); err != nil {
		return ErrInvalidPayload
	}
	return nil
}

var ErrInvalidPayload = errors.New("invalid payload")
