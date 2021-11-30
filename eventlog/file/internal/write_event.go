package internal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/romshark/eventlog/eventlog"
)

type Hasher interface {
	io.Writer
	Reset()
	Sum64() uint64
}

type SyncWriter interface {
	WriteAt(data []byte, offset int64) (int, error)
	Sync() error
}

func WriteEvent(
	writer SyncWriter,
	buffer []byte,
	checksum uint64,
	offset int64,
	event eventlog.Event,
	conf Config,
) (written int, err error) {
	if err := conf.VerifyPayloadLen(event.PayloadJSON); err != nil {
		return 0, err
	}

	if len(buffer) < 8 {
		buffer = make([]byte, 8)
	}
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
	binary.LittleEndian.PutUint64(buf8, event.Timestamp)
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
		if write(event.Label, "label") {
			return
		}
	}

	// Write payload
	if write(event.PayloadJSON, "payload") {
		return
	}

	// Write previous version (8 bytes)
	binary.LittleEndian.PutUint64(buf8, event.VersionPrevious)
	if write(buf8, "previous version") {
		return
	}

	err = writer.Sync()
	return
}
