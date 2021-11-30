package internal

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/romshark/eventlog/eventlog"
)

// WriteFileHeader writes the file header
func WriteFileHeader(
	writer SyncWriter,
	hasher Hasher,
	creation time.Time,
	metadataJSON []byte,
	conf Config,
) (written int, err error) {
	if err := conf.VerifyPayloadLen(metadataJSON); err != nil {
		return 0, err
	}

	var metaChecksum uint64
	if metaChecksum, err = Checksum(
		nil,
		hasher,
		uint64(creation.Unix()),
		nil,
		metadataJSON,
		0,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	// Write version
	bufU32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufU32, SupportedProtoVersion)
	if _, err := writer.WriteAt(bufU32, 0); err != nil {
		return 0, fmt.Errorf("writing header version: %w", err)
	}
	written += 4

	metaEntryLen, err := WriteEvent(
		writer,
		nil,
		metaChecksum,
		4,
		eventlog.Event{
			Timestamp: uint64(creation.Unix()),
			EventData: eventlog.EventData{
				PayloadJSON: metadataJSON,
			},
		},
		conf,
	)
	written += metaEntryLen
	if err != nil {
		return written, fmt.Errorf("writing metadata: %w", err)
	}

	return written, nil
}
