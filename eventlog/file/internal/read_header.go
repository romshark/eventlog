package internal

import (
	"encoding/binary"
	"fmt"
)

// ReadHeader reads a file header
//
// WARNING: buffer must be at least 4 bytes long.
func ReadHeader(
	buffer []byte,
	reader OffsetReader,
	checkVersion func(uint32) error,
) error {
	buf4 := buffer[:4]
	n, err := reader.ReadAt(buf4, 0)
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf(
			"reading version header (expected: 4, actual: %d)", n,
		)
	}
	return checkVersion(binary.LittleEndian.Uint32(buf4))
}

type OffsetReader interface {
	ReadAt(buf []byte, offset int64) (read int, err error)
}
