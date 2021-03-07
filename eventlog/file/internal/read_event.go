package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/romshark/eventlog/eventlog"
)

type ReaderConf struct {
	MinPayloadLen uint32
	MaxPayloadLen uint32
}

// ReadEvent reads an event entry from the given reader at the given offset.
// Checks the read entry's integrity by validating the read checksum.
// Returns io.EOF when there's nothing more to read.
//
// WARNING: returned label and payload slices both reference the given buffer.
// WARNING: buffer must be at least the size of the sum of the
// maximum possible payload and label lengths.
func ReadEvent(
	buffer ReadBuffer,
	reader OffsetReader,
	hasher Hasher,
	offset int64,
	conf ReaderConf,
) (
	checksum uint64,
	timestamp uint64,
	label []byte,
	payload []byte,
	bytesRead int64,
	err error,
) {
	buffer.MustValidate(conf)
	hasher.Reset()

	buf8 := buffer[:8]
	buf4 := buffer[:4]
	buf2 := buffer[:2]

	writeHash := func(data []byte, field string) (failed bool) {
		var n int
		if n, err = hasher.Write(data); err != nil {
			err = fmt.Errorf("writing hash (%s): %w", field, err)
			return true
		} else if n != len(data) {
			err = fmt.Errorf(
				"unexpected written to hasher (expected: %d; written: %d)",
				len(data), n,
			)
			return true
		}
		return false
	}

	read := func(buffer []byte, fieldName string, hash bool) (failed bool) {
		var n int
		if n, err = reader.ReadAt(buffer, offset); err != nil {
			err = fmt.Errorf(
				"reading %s at offset %d: %w",
				fieldName, offset, err,
			)
			return true
		} else if n != len(buffer) {
			err = eventlog.ErrInvalidOffset
			return true
		}
		if hash {
			if writeHash(buffer, fieldName) {
				return true
			}
		}
		bytesRead += int64(n)
		offset += int64(n)
		return false
	}

	// Read checksum (8 bytes)
	if read(buf8, "checksum", false) {
		if errors.Is(err, io.EOF) {
			err = io.EOF
		}
		return
	}
	checksum = binary.LittleEndian.Uint64(buf8)

	// Read timestamp (8 bytes)
	if read(buf8, "timestamp", true) {
		return
	}
	timestamp = binary.LittleEndian.Uint64(buf8)

	// Read label length (2 bytes)
	if read(buf2, "label length", true) {
		return
	}
	labelLen := binary.LittleEndian.Uint16(buf2)

	// Read payload length (4 bytes)
	if read(buf4, "payload length", true) {
		return
	}
	payloadLen := binary.LittleEndian.Uint32(buf4)

	// Check payload length
	if payloadLen < conf.MinPayloadLen ||
		payloadLen > conf.MaxPayloadLen {
		// Invalid payload length indicates wrong offset
		err = eventlog.ErrInvalidOffset
		return
	}

	if labelLen > 0 {
		// Read label
		label = buffer[:labelLen]
		if read(label, "label", true) {
			return
		}
	}

	// Read payload
	payload = buffer[labelLen : uint32(labelLen)+payloadLen]
	if read(payload, "payload", true) {
		return
	}

	// Verify integrity of the read entry
	if actualChecksum := hasher.Sum64(); checksum != actualChecksum {
		err = eventlog.ErrInvalidOffset
		return
	}

	return
}

type ReadBuffer []byte

func (b ReadBuffer) MustValidate(conf ReaderConf) {
	requiredBufferLen := conf.MaxPayloadLen +
		uint32(256) // Max label length
	if uint32(len(b)) < requiredBufferLen {
		panic(fmt.Errorf(
			"buffer too small (given: %d; required: %d)",
			len(b), requiredBufferLen,
		))
	}
}
