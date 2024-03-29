package internal

import (
	"encoding/binary"
	"fmt"
)

// Checksum computes the 64-bit checksum hash for the given event.
func Checksum(
	buffer []byte,
	hasher Hasher,
	timestamp uint64,
	label []byte,
	payload []byte,
	versionPrevious uint64,
) (checksum uint64, err error) {
	if len(buffer) < 8 {
		buffer = make([]byte, 8)
	}

	hasher.Reset()

	write := func(data []byte) (failed bool) {
		var n int
		if n, err = hasher.Write(data); err != nil {
			return true
		} else if n != len(data) {
			err = fmt.Errorf(
				"unexpected written: (expected: %d; written: %d)",
				len(data), n,
			)
			return true
		}
		return false
	}

	buf8 := buffer[:8]
	binary.LittleEndian.PutUint64(buf8, timestamp)
	if write(buf8) {
		return
	}

	buf2 := buffer[:2]
	binary.LittleEndian.PutUint16(buf2, uint16(len(label)))
	if write(buf2) {
		return
	}

	buf4 := buffer[:4]
	binary.LittleEndian.PutUint32(buf4, uint32(len(payload)))
	if write(buf4) {
		return
	}
	if len(label) > 0 && write(label) {
		return
	}
	if write(payload) {
		return
	}

	binary.LittleEndian.PutUint64(buf8, versionPrevious)
	if write(buf8) {
		return
	}

	return hasher.Sum64(), nil
}
