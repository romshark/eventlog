package file

import (
	"context"
	"fmt"
	"io"

	"github.com/romshark/eventlog/eventlog"
)

func CheckIntegrity(
	ctx context.Context,
	reader reader,
	buf []byte,
	onEntry func(
		timestamp uint64,
		payload []byte,
		offset int64,
	) error,
) error {
	if buf == nil {
		buf = make([]byte, MaxPayloadLen)
	}

	if err := readHeader(reader, buf); err != nil {
		return err
	}

	var previousTime uint64

	for i := int64(4); ; {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, tm, pl, err := readEntry(reader, buf, i)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("reading entry at offset %d: %w", i, err)
		}

		if tm < previousTime {
			return fmt.Errorf(
				"invalid timestamp (%d) at %d greater than previous (%d)",
				tm, i, previousTime,
			)
		}
		previousTime = tm

		if err := eventlog.ValidatePayloadJSON(pl); err != nil {
			return fmt.Errorf("invalid payload %d: %w", i, err)
		}

		if err := onEntry(tm, pl, i); err != nil {
			return err
		}
		i += int64(n)
	}

	return nil
}
