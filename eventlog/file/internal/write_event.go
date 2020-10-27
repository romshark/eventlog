package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	"github.com/romshark/eventlog/eventlog"
)

type Hasher interface {
	io.Writer
	Sum64() uint64
}

type Writer interface {
	WriteAt(data []byte, offset int64) (int, error)
	Sync() error
}

func WriteEvent(
	writer Writer,
	buffer []byte,
	checksum uint64,
	offset int64,
	timestamp uint64,
	event eventlog.Event,
) (written int, err error) {
	buf8 := buffer[:8]
	buf4 := buffer[:4]
	buf2 := buffer[:2]

	var n int

	write := func(data []byte, field string) (failed bool) {
		if n, err = writer.WriteAt(data, offset); err != nil {
			return
		} else if n != len(data) {
			err = fmt.Errorf(
				"writing %s, unexpectedly wrote: %d (expected: %d)",
				field, n, len(data),
			)
			return
		}
		written += n
		offset += int64(n)
		return false
	}

	// Write checksum (8 bytes)
	binary.LittleEndian.PutUint64(buf8, checksum)
	if write(buf8, "checksum") {
		return
	}

	// Write timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf8, timestamp)
	if write(buf8, "timestamp") {
		return
	}

	// Write label length (2 bytes)
	binary.LittleEndian.PutUint16(buf2, uint16(len(event.Label)))
	if write(buf2, "label length") {
		return
	}

	// Write payload length (4 bytes)
	binary.LittleEndian.PutUint32(buf4, uint32(len(event.PayloadJSON)))
	if write(buf4, "payload length") {
		return
	}

	// Write label
	if len(event.Label) > 0 {
		if write(UnsafeS2B(event.Label), "label") {
			return
		}
	}

	// Write payload
	if write(event.PayloadJSON, "payload") {
		return
	}

	err = writer.Sync()
	return
}

func UnsafeS2B(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}
