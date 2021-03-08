package internal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/romshark/eventlog/eventlog"
)

const MaxMetaDataLen = 4294967295

// WriteFileHeader writes the file header
func WriteFileHeader(
	writer SyncWriter,
	hasher Hasher,
	metadata map[string]string,
) (written int, err error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return written, fmt.Errorf("encoding metadata JSON: %w", err)
	}

	if l := len(metadataJSON); l > MaxMetaDataLen {
		return written, fmt.Errorf(
			"max meta info JSON length (%d) exceeded (%d)",
			MaxMetaDataLen, l,
		)
	}

	var metaChecksum uint64
	if metaChecksum, err = Checksum(
		nil,
		hasher,
		0,
		nil,
		metadataJSON,
	); err != nil {
		err = fmt.Errorf("computing checksum: %w", err)
		return
	}

	// Write version
	bufU32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bufU32, 4)
	if _, err := writer.WriteAt(bufU32, 0); err != nil {
		return 0, fmt.Errorf("writing header version: %w", err)
	}
	written += 4

	metaEntryLen, err := WriteEvent(
		writer,
		nil,
		metaChecksum,
		4, 0,
		eventlog.Event{PayloadJSON: metadataJSON},
	)
	written += metaEntryLen
	if err != nil {
		return written, fmt.Errorf("writing metadata: %w", err)
	}

	return written, nil
}
