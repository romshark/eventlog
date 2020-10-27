package file_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal"
	bin "github.com/romshark/eventlog/internal/bin"
	"github.com/stretchr/testify/require"
)

func TestCheckIntegrity(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),  // checksum
		e2.Timestamp,    // timestamp
		e2.LabelLen(),   // label length
		e2.PayloadLen(), // payload length
		e2.Label,        // label
		e2.Payload,      // payload
	)

	cbCalled := 0
	r := require.New(t)
	r.NoError(file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			switch cbCalled {
			case 1:
				e1 := validEvent1()
				r.Equal(e1.Timestamp, timestamp)
				r.Equal(e1.Label, string(label))
				r.Equal(e1.Payload, string(payload))
				r.Equal(int64(file.FileHeaderLen), offset)
			case 2:
				e2 := validEvent2()
				r.Equal(e2.Timestamp, timestamp)
				r.Equal(e2.Label, string(label))
				r.Equal(e2.Payload, string(payload))
				r.Equal(int64(file.FileHeaderLen)+e1.Len(), offset)
			}
			return nil
		},
	))
	r.Equal(2, cbCalled)
}

func TestCheckIntegrityUnsupportedVersion(t *testing.T) {
	f := bin.Compose(uint32(file.SupportedProtoVersion + 1))
	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		fmt.Sprintf(
			"unsupported file version (%d)",
			file.SupportedProtoVersion+1,
		),
		err.Error(),
	)
	r.Zero(cbCalled)
}

func TestCheckIntegrityInvalidTimestamps(t *testing.T) {
	e1 := validEvent1()
	e1.Timestamp = uint64(9999999999)

	e2 := validEvent2()
	e2.Timestamp = uint64(8888888888)

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),  // checksum
		e2.Timestamp,    // timestamp
		e2.LabelLen(),   // label length
		e2.PayloadLen(), // payload length
		e2.Label,        // label
		e2.Payload,      // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		fmt.Sprintf(
			"invalid timestamp (8888888888) at offset %d "+
				"greater than previous (9999999999)",
			int64(file.FileHeaderLen)+e1.Len(),
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityInvalidJSONPayload(t *testing.T) {
	for _, tt := range []string{
		`{     }`,
		`["array", "is", "illegal"]`,
		`   42   `,
		`"foo   "`,
		`null    `,
		`false   `,
		`{x:"syntax error"}`,
	} {
		t.Run(tt, func(t *testing.T) {
			e1 := validEvent1()
			e2 := validEvent2()
			e2.Payload = tt

			f := bin.Compose(
				// header
				uint32(file.SupportedProtoVersion),
				// first entry
				e1.Checksum(t),  // checksum
				e1.Timestamp,    // timestamp
				e1.LabelLen(),   // label length
				e1.PayloadLen(), // payload length
				e1.Label,        // label
				e1.Payload,      // payload
				// second entry
				e2.Checksum(t),  // checksum
				e2.Timestamp,    // timestamp
				e2.LabelLen(),   // label length
				e2.PayloadLen(), // payload length
				e2.Label,        // label
				e2.Payload,      // payload (invalid!)
			)

			cbCalled := 0
			r := require.New(t)
			err := file.CheckIntegrity(
				context.Background(),
				newBuffer(),
				FakeSrc(f),
				func(
					offset int64,
					checksum uint64,
					timestamp uint64,
					label []byte,
					payload []byte,
				) error {
					cbCalled++
					return nil
				},
			)

			r.Error(err)
			r.True(
				errors.Is(err, eventlog.ErrInvalidPayload),
				"unexpected error: (%T) %s", err, err.Error(),
			)
			r.True(
				strings.HasPrefix(
					err.Error(),
					`invalid payload at offset 33:`,
				),
				"unexpected error message: %q", err.Error(),
			)
			r.Equal(1, cbCalled)
		})
	}
}

func TestCheckIntegrityLabelLengthTooSmall(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),          // checksum
		e2.Timestamp,            // timestamp
		uint16(len(e2.Label)-1), // label length (invalid!)
		e2.PayloadLen(),         // payload length
		e2.Label,                // label
		e2.Payload,              // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"reading entry at offset %d: invalid offset",
			int64(file.FileHeaderLen)+e1.Len(),
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityLabelLengthTooLarge(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),          // checksum
		e2.Timestamp,            // timestamp
		uint16(len(e2.Label)+1), // label length (invalid!)
		e2.PayloadLen(),         // payload length
		e2.Label,                // label
		e2.Payload,              // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"reading entry at offset %d: invalid offset",
			int64(file.FileHeaderLen)+e1.Len(),
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadLengthTooSmall(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),            // checksum
		e2.Timestamp,              // timestamp
		e2.LabelLen(),             // label length
		uint32(len(e2.Payload)-1), // payload length (invalid!)
		e2.Label,                  // label
		e2.Payload,                // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"reading entry at offset %d: invalid offset",
			int64(file.FileHeaderLen)+e1.Len(),
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadLengthTooLarge(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),            // checksum
		e2.Timestamp,              // timestamp
		e2.LabelLen(),             // label length
		uint32(len(e2.Payload)+1), // payload length (invalid!)
		e2.Label,                  // label
		e2.Payload,                // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"reading entry at offset %d: invalid offset",
			int64(file.FileHeaderLen)+e1.Len(),
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadTooSmall(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()
	invalidPayload := e2.Payload
	invalidPayload = invalidPayload[:len(invalidPayload)-1]
	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t),  // checksum
		e2.Timestamp,    // timestamp
		e2.LabelLen(),   // label length
		e2.PayloadLen(), // payload length
		e2.Label,        // label
		invalidPayload,  // payload (too small!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("reading entry at offset 33: invalid offset", err.Error())
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMismatchingChecksum(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t)-1, // checksum (mismatching!)
		e2.Timestamp,     // timestamp
		e2.LabelLen(),    // label length
		e2.PayloadLen(),  // payload length
		e2.Label,         // label
		e2.Payload,       // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidOffset),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedTimestamp(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t), // checksum
		[]byte{0, 1},   // timestamp (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("reading entry at offset 33: invalid offset", err.Error())
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedChecksum(t *testing.T) {
	e1 := validEvent1()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		[]byte{0, 0}, // checksum (malformed)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("reading entry at offset 33: invalid offset", err.Error())
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedLabelLength(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t), // checksum
		e2.Timestamp,   // timestamp
		[]byte{1},      // label length (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("reading entry at offset 33: invalid offset", err.Error())
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedPayloadLength(t *testing.T) {
	e1 := validEvent1()
	e2 := validEvent2()

	f := bin.Compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		e1.Checksum(t),  // checksum
		e1.Timestamp,    // timestamp
		e1.LabelLen(),   // label length
		e1.PayloadLen(), // payload length
		e1.Label,        // label
		e1.Payload,      // payload
		// second entry
		e2.Checksum(t), // checksum
		e2.Timestamp,   // timestamp
		e2.LabelLen(),  // label length
		[]byte{0, 1},   // payload length (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		newBuffer(),
		FakeSrc(f),
		func(
			offset int64,
			checksum uint64,
			timestamp uint64,
			label []byte,
			payload []byte,
		) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("reading entry at offset 33: invalid offset", err.Error())
	r.Equal(1, cbCalled)
}

type FakeSrc []byte

func (f FakeSrc) ReadAt(buf []byte, offset int64) (read int, err error) {
	if offset >= int64(len(f)) {
		return 0, io.EOF
	}
	d := f[offset:]
	if len(buf) > len(d) {
		buf = buf[:len(d)]
	}
	copy(buf, d)
	return len(buf), nil
}

func TestFakeSrc(t *testing.T) {
	r := require.New(t)
	f := FakeSrc("0123456789")

	b := make([]byte, 4)
	n, err := f.ReadAt(b, 0)
	r.NoError(err)
	r.Equal(4, n)
	r.Equal("0123", string(b))

	b = make([]byte, 20)
	n, err = f.ReadAt(b, 0)
	r.NoError(err)
	r.Equal(10, n)
	r.Equal("0123456789", string(b[:n]))

	b = make([]byte, 20)
	n, err = f.ReadAt(b, 5)
	r.NoError(err)
	r.Equal(5, n)
	r.Equal("56789", string(b[:n]))

	b = make([]byte, 2)
	n, err = f.ReadAt(b, 6)
	r.NoError(err)
	r.Equal(2, n)
	r.Equal("67", string(b[:n]))

	b = make([]byte, 2)
	n, err = f.ReadAt(b, 10)
	r.Error(err)
	r.Error(io.EOF, err)
	r.Zero(n)
	r.Equal([]byte{0, 0}, b)
}

func validEvent1() TestEvent {
	return TestEvent{
		Timestamp: 8888888888,
		Label:     "",
		Payload:   `{"x":0}`,
	}
}

func validEvent2() TestEvent {
	return TestEvent{
		Timestamp: 9999999999,
		Label:     "foo_bar",
		Payload:   `{"medium":"size"}`,
	}
}

type TestEvent struct {
	Timestamp uint64
	Label     string
	Payload   string
}

func (e TestEvent) LabelLen() uint16 {
	return uint16(len(e.Label))
}

func (e TestEvent) PayloadLen() uint32 {
	return uint32(len(e.Payload))
}

func (e TestEvent) Len() int64 {
	return int64(22 + len(e.Label) + len(e.Payload))
}

func (e TestEvent) Checksum(t *testing.T) uint64 {
	c := internal.ChecksumT(t, e.Timestamp, e.Label, e.Payload)
	return c
}

func newBuffer() internal.ReadBuffer {
	return make([]byte, file.MinReadBufferLen)
}
