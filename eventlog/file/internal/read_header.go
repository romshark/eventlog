package internal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// ReadHeader reads the header of a log file
func ReadHeader(
	buffer ReadBuffer,
	reader OffsetReader,
	hasher Hasher,
	conf Config,
	onMeta func(field, value string) error,
) (headerLen int64, err error) {
	buffer.MustValidate(conf)

	buf4 := buffer[:4]

	// Read version
	if _, err := reader.ReadAt(buf4, 0); err != nil {
		return 0, fmt.Errorf("reading version: %w", err)
	}
	if v := binary.LittleEndian.Uint32(buf4); v != SupportedProtoVersion {
		return headerLen, fmt.Errorf("unsupported file version (%d)", v)
	}
	headerLen += 4

	_, e, ln, err := ReadEvent(
		buffer, reader, hasher, 4, conf,
	)
	if err != nil {
		return 0, fmt.Errorf("reading metadata: %w", err)
	}
	headerLen += ln

	if onMeta != nil {
		var metadata map[string]string
		if err := json.Unmarshal(e.PayloadJSON, &metadata); err != nil {
			return 0, fmt.Errorf("decoding metadata JSON: %w", err)
		}
		for f, v := range metadata {
			if err := onMeta(f, v); err != nil {
				return 0, err
			}
		}
	}

	return headerLen, nil
}

type OffsetLenReader interface {
	OffsetReader
	Len() (uint64, error)
}

type OffsetReader interface {
	ReadAt(buf []byte, offset int64) (read int, err error)
}
