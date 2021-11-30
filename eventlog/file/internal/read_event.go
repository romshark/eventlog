package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/romshark/eventlog/eventlog"
)

const SupportedProtoVersion = 5

// ReadEvent reads an event entry from the given reader at the offset (version).
// Checks the read entry's integrity by validating the read checksum.
// Returns io.EOF when there's nothing more to read.
// event.VersionNext will always remain zero as it's not part of an entry.
//
// WARNING: returned label and payload slices both reference the given buffer.
// WARNING: buffer must be at least the size of the sum of the
// maximum possible payload and label lengths.
func ReadEvent(
	buffer ReadBuffer,
	reader OffsetReader,
	hasher Hasher,
	offset int64,
	conf Config,
) (
	checksum uint64,
	event eventlog.Event,
	bytesRead int64,
	err error,
) {
	buffer.MustValidate(conf)
	hasher.Reset()

	event.Version = uint64(offset)

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
			err = eventlog.ErrInvalidVersion
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
	event.Timestamp = binary.LittleEndian.Uint64(buf8)

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
	if payloadLen < uint32(conf.MinPayloadLen) ||
		payloadLen > uint32(conf.MaxPayloadLen) {
		// Invalid payload length indicates wrong version
		err = eventlog.ErrInvalidVersion
		return
	}

	if labelLen > 0 {
		// Read label
		event.Label = buffer[8 : 8+labelLen]
		if read(event.Label, "label", true) {
			return
		}
	}

	// Read payload
	event.PayloadJSON = buffer[8+labelLen : 8+uint32(labelLen)+payloadLen]
	if read(event.PayloadJSON, "payload", true) {
		return
	}

	// Read previous version (8 bytes)
	if read(buf8, "previous version", true) {
		return
	}
	event.VersionPrevious = binary.LittleEndian.Uint64(buf8)

	// Verify integrity of the read entry
	if actualChecksum := hasher.Sum64(); checksum != actualChecksum {
		err = eventlog.ErrInvalidVersion
		return
	}

	return
}

type ReadBuffer []byte

func (b ReadBuffer) MustValidate(conf Config) {
	requiredBufferLen := conf.MaxPayloadLen +
		8 + // Previous version
		256 // Max label length
	if len(b) < requiredBufferLen {
		panic(fmt.Errorf(
			"buffer too small (given: %d; required: %d)",
			len(b), requiredBufferLen,
		))
	}
}
