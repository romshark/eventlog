package msgcodec

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/internal"
)

// EncodeBinary encodes the events into a binary message
// in the format: [label_len(2)][payload_len(4)][label][payload]
func EncodeBinary(events ...eventlog.EventData) ([]byte, error) {
	if len(events) < 1 {
		return nil, nil
	}
	totalMsgLen := 0
	for _, e := range events {
		if len(e.PayloadJSON) < 1 {
			return nil, eventlog.ErrInvalidPayload
		}
		if len(e.Label) > internal.MaxLabelLen {
			return nil, eventlog.ErrLabelTooLong
		}
		// Calculate payload length
		totalMsgLen += 6 /* header (2 label length + 4 payload length) */ +
			len(e.Label) +
			len(e.PayloadJSON)
	}

	buf4 := make([]byte, 4)
	buf2 := buf4[:2]

	buf := bytes.Buffer{}
	buf.Grow(totalMsgLen)
	for _, e := range events {
		// Write label length
		binary.LittleEndian.PutUint16(buf2, uint16(len(e.Label)))
		_, _ = buf.Write(buf2)

		// Write payload length
		binary.LittleEndian.PutUint32(buf4, uint32(len(e.PayloadJSON)))
		_, _ = buf.Write(buf4)

		// Write label
		if len(e.Label) > 0 {
			_, _ = buf.Write(e.Label)
		}

		// Write Payload
		_, _ = buf.Write(e.PayloadJSON)
	}

	return buf.Bytes(), nil
}

// scanBytesNumBinary scans the bytes checking for errors
// returning the number of events encoded in the message
func scanBytesNumBinary(bytes []byte) (num int, err error) {
	const minLen = 6 /* header (label len + payload len) */ +
		len(`{"x":0}`) /* min JSON payload len */

	if len(bytes) < minLen {
		return 0, ErrMalformedMessage
	}

	for offset := 0; offset < len(bytes); num++ {
		// Check whether enough bytes are left
		// to read the smallest possible message
		if bytesLeft := len(bytes) - offset - minLen; bytesLeft == 0 {
			num++
			break
		} else if bytesLeft < 0 {
			// Malformed message
			return 0, ErrMalformedMessage
		}
		b := bytes[offset:]

		// Read label len header
		labelLen := binary.LittleEndian.Uint16(b[:2])

		// Read payload len header
		payloadLen := binary.LittleEndian.Uint32(b[2:6])

		// Check whether enough bytes are left to read the message
		msgLen := (6 + int(labelLen) + int(payloadLen))
		if bytesLeft := len(b) - msgLen; bytesLeft < 0 {
			// Malformed message
			return 0, ErrMalformedMessage
		}
		offset += msgLen
	}
	return
}

// ScanBytesBinary scans events encoded in binary format
// calling onLen as soon as the number of events is known
// and calling onEvent for every scanned event.
func ScanBytesBinary(
	bytes []byte,
	onNum func(int) error,
	onEvent func(label []byte, payloadJSON []byte) error,
) error {
	n, err := scanBytesNumBinary(bytes)
	if err != nil {
		return err
	}
	if err := onNum(n); err != nil {
		return err
	}

	for offset := 0; ; {
		b := bytes[offset:]
		if len(b) < 1 {
			break
		}

		// Read label len header
		labelLen := binary.LittleEndian.Uint16(b[:2])

		// Read payload len header
		payloadLen := binary.LittleEndian.Uint32(b[2:6])

		if len(b) < 6+int(labelLen)+int(payloadLen) {
			// Less bytes left than necessary
			return ErrMalformedMessage
		}

		payloadOffset := 6 + uint32(labelLen)

		if err := onEvent(
			b[6:payloadOffset],
			b[payloadOffset:payloadOffset+payloadLen],
		); err != nil {
			return err
		}
		offset += 6 + int(labelLen) + int(payloadLen)
	}
	return nil
}

var ErrMalformedMessage = errors.New("malformed message")
