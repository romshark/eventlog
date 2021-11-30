package file

import (
	"fmt"
	"io"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file/internal"

	xxhash "github.com/cespare/xxhash/v2"
)

const MinReadBufferLen = MaxLabelLen + MaxPayloadLen + 8

func CheckIntegrity(
	buffer internal.ReadBuffer,
	reader internal.OffsetLenReader,
	onEntry func(checksum uint64, event eventlog.Event) error,
) error {
	hash := xxhash.New()

	headerLen, err := internal.ReadHeader(
		buffer,
		reader,
		hash,
		config,
		func(field, value string) error {
			// Ignore meta fields, but make sure they're parsed.
			return nil
		},
	)
	if err != nil {
		return err
	}

	var previousTime uint64
	var versionPrevious uint64

	makeErr := func(offset int64, format string, v ...interface{}) error {
		return fmt.Errorf(
			fmt.Sprintf("error at offset %d: ", offset)+format,
			v...,
		)
	}

	for i := int64(headerLen); ; {
		checksum, event, n, err :=
			internal.ReadEvent(buffer, reader, hash, i, config)
		if err == io.EOF {
			break
		} else if err != nil {
			return makeErr(i, "reading entry: %w", err)
		}

		if event.Timestamp < previousTime {
			return makeErr(
				i, "invalid timestamp (%d) greater than previous (%d)",
				event.Timestamp, previousTime,
			)
		}
		previousTime = event.Timestamp

		if err := (eventlog.EventData{
			Label:       event.Label,
			PayloadJSON: event.PayloadJSON,
		}).Validate(); err != nil {
			return makeErr(i, "invalid payload: %w", err)
		}

		i += int64(n)

		ln, err := reader.Len()
		if err != nil {
			return makeErr(i-int64(n), "reading source length: %w", err)
		}
		if uint64(i) < ln {
			event.VersionNext = uint64(i)
		}

		if event.VersionPrevious != versionPrevious {
			return makeErr(i-int64(n),
				"invalid previous version (%d), expected %d",
				event.VersionPrevious, versionPrevious,
			)
		}
		versionPrevious = event.Version

		if err := onEntry(checksum, event); err != nil {
			return err
		}
	}

	return nil
}
