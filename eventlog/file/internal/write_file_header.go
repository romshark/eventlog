package internal

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteFileHeader(
	buffer []byte,
	writer io.Writer,
	supportedProtoVersion uint32,
) error {
	// New file, write header
	bufUint32 := buffer[:4]
	binary.LittleEndian.PutUint32(bufUint32, supportedProtoVersion)
	if _, err := writer.Write(bufUint32); err != nil {
		return fmt.Errorf("writing header version: %w", err)
	}
	return nil
}
