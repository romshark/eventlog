package file_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/romshark/eventlog/eventlog/file/internal/test"
	bin "github.com/romshark/eventlog/internal/bin"

	"github.com/stretchr/testify/require"
)

func TestCheckIntegrity(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2, e3 := vl[0], vl[1], vl[2]

	fakeSrc, _, hlen := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		e2.Timestamp,         // Timestamp
		test.LabelLen(e2),    // Label length
		test.PayloadLen(e2),  // Payload length
		e2.Label,             // Label
		e2.PayloadJSON,       // Payload
		e2.VersionPrevious,   // Previous version
		// Third entry
		test.Checksum(t, e3), // Checksum
		e3.Timestamp,         // Timestamp
		test.LabelLen(e3),    // Label length
		test.PayloadLen(e3),  // Payload length
		e3.Label,             // Label
		e3.PayloadJSON,       // Payload
		e3.VersionPrevious,   // Previous version
	)

	cbCalled := 0
	r := require.New(t)
	err := file.CheckIntegrity(
		test.NewBuffer(),
		fakeSrc,
		func(checksum uint64, e eventlog.Event) error {
			cbCalled++
			switch cbCalled {
			case 1:
				e1 := test.ValidLog(t)[0]
				r.Equal(e1.Timestamp, e.Timestamp)
				r.Equal(string(e1.Label), string(e.Label))
				r.Equal(string(e1.PayloadJSON), string(e.PayloadJSON))
				r.Equal(e1.VersionPrevious, e.VersionPrevious)
				r.Equal(uint64(hlen), e.Version)
				r.Equal(test.ValidLog(t)[1].Version, e.VersionNext)

			case 2:
				e2 := test.ValidLog(t)[1]
				r.Equal(e2.Timestamp, e.Timestamp)
				r.Equal(string(e2.Label), string(e.Label))
				r.Equal(string(e2.PayloadJSON), string(e.PayloadJSON))
				r.Equal(e2.VersionPrevious, e.VersionPrevious)
				r.Equal(
					uint64(hlen+test.EventLen(test.ValidLog(t)[0])),
					e.Version,
				)
				r.Equal(test.ValidLog(t)[2].Version, e.VersionNext)

			case 3:
				e3 := test.ValidLog(t)[2]
				r.Equal(e3.Timestamp, e.Timestamp)
				r.Equal(string(e3.Label), string(e.Label))
				r.Equal(string(e3.PayloadJSON), string(e.PayloadJSON))
				r.Equal(e3.VersionPrevious, e.VersionPrevious)
				r.Equal(
					uint64(hlen+
						test.EventLen(test.ValidLog(t)[0])+
						test.EventLen(test.ValidLog(t)[1])),
					e.Version,
				)
				r.Zero(e.VersionNext)
			}
			return nil
		},
	)
	r.NoError(err)
	r.Equal(3, cbCalled)
}

func TestCheckIntegrityUnsupportedVersion(t *testing.T) {
	f := bin.Compose(uint32(file.SupportedProtoVersion + 1))

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
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
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	e1.Timestamp = uint64(9999999999)
	e2.Timestamp = uint64(8888888888)

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		e2.Timestamp,         // Timestamp
		test.LabelLen(e2),    // Label length
		test.PayloadLen(e2),  // Payload length
		e2.Label,             // Label
		e2.PayloadJSON,       // Payload
		e2.VersionPrevious,   // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: invalid timestamp (8888888888) "+
				"greater than previous (9999999999)",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityWrongPreviousVersion(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	e2.VersionPrevious = uint64(e1.Version + 42)

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		e2.Timestamp,         // Timestamp
		test.LabelLen(e2),    // Label length
		test.PayloadLen(e2),  // Payload length
		e2.Label,             // Label
		e2.PayloadJSON,       // Payload
		e2.VersionPrevious,   // Previous version (invalid!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: invalid previous version (%d), "+
				"expected %d",
			e2.Version, e1.Version+42, e1.Version,
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
			vl := test.ValidLog(t)

			e1, e2 := vl[0], vl[1]
			e2.PayloadJSON = []byte(tt)

			f, _, _ := test.ComposeWithValidHeader(t,
				// First entry
				test.Checksum(t, e1), // Checksum
				e1.Timestamp,         // Timestamp
				test.LabelLen(e1),    // Label length
				test.PayloadLen(e1),  // Payload length
				e1.Label,             // Label
				e1.PayloadJSON,       // Payload
				e1.VersionPrevious,   // Previous version
				// Second entry
				test.Checksum(t, e2), // Checksum
				e2.Timestamp,         // Timestamp
				test.LabelLen(e2),    // Label length
				test.PayloadLen(e2),  // Payload length
				e2.Label,             // Label
				e2.PayloadJSON,       // Payload (invalid!)
				e2.VersionPrevious,   // Previous version
			)

			cbCalled := 0
			r := require.New(t)
			err := file.CheckIntegrity(
				test.NewBuffer(),
				test.FakeSrc(f),
				func(checksum uint64, _ eventlog.Event) error {
					cbCalled++
					return nil
				},
			)

			r.Error(err)
			r.True(
				errors.Is(err, eventlog.ErrInvalidPayload),
				"unexpected error: (%T) %s", err, err.Error(),
			)
			r.Equal(
				fmt.Sprintf(
					`error at offset %d: invalid payload: invalid payload`,
					e2.Version,
				),
				err.Error(),
			)
			r.Equal(1, cbCalled)
		})
	}
}

func TestCheckIntegrityLabelLengthTooSmall(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2),    // Checksum
		e2.Timestamp,            // Timestamp
		uint16(len(e2.Label)-1), // Label length (invalid!)
		test.PayloadLen(e2),     // Payload length
		e2.Label,                // Label
		e2.PayloadJSON,          // Payload
		e2.VersionPrevious,      // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityLabelLengthTooLarge(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2),    // Checksum
		e2.Timestamp,            // Timestamp
		uint16(len(e2.Label)+1), // Label length (invalid!)
		test.PayloadLen(e2),     // Payload length
		e2.Label,                // Label
		e2.PayloadJSON,          // Payload
		e2.VersionPrevious,      // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadLengthTooSmall(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2),  // Checksum
		e2.Timestamp,          // Timestamp
		test.LabelLen(e2),     // Label length
		test.PayloadLen(e2)-1, // Payload length (invalid!)
		e2.Label,              // Label
		e2.PayloadJSON,        // Payload
		e2.VersionPrevious,    // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadLengthTooLarge(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2),  // Checksum
		e2.Timestamp,          // Timestamp
		test.LabelLen(e2),     // Label length
		test.PayloadLen(e2)+1, // Payload length (invalid!)
		e2.Label,              // Label
		e2.PayloadJSON,        // Payload
		e2.VersionPrevious,    // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMismatchingChecksum(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2)-1, // Checksum (mismatching!)
		e2.Timestamp,           // Timestamp
		test.LabelLen(e2),      // Label length
		test.PayloadLen(e2),    // Payload length
		e2.Label,               // Label
		e2.PayloadJSON,         // Payload
		e2.VersionPrevious,     // Previous version
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedTimestamp(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		[]byte{0, 1},         // Timestamp (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedChecksum(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		[]byte{0, 0}, // Checksum (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedLabelLength(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		e2.Timestamp,         // Timestamp
		[]byte{1},            // Label length (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedPayloadLength(t *testing.T) {
	vl := test.ValidLog(t)
	e1, e2 := vl[0], vl[1]

	f, _, _ := test.ComposeWithValidHeader(t,
		// First entry
		test.Checksum(t, e1), // Checksum
		e1.Timestamp,         // Timestamp
		test.LabelLen(e1),    // Label length
		test.PayloadLen(e1),  // Payload length
		e1.Label,             // Label
		e1.PayloadJSON,       // Payload
		e1.VersionPrevious,   // Previous version
		// Second entry
		test.Checksum(t, e2), // Checksum
		e2.Timestamp,         // Timestamp
		test.LabelLen(e2),    // Label length
		[]byte{0, 1},         // Payload length (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		test.NewBuffer(),
		test.FakeSrc(f),
		func(checksum uint64, _ eventlog.Event) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(
		errors.Is(err, eventlog.ErrInvalidVersion),
		"unexpected error: (%T) %s", err, err.Error(),
	)
	r.Equal(
		fmt.Sprintf(
			"error at offset %d: reading entry: invalid version",
			e2.Version,
		),
		err.Error(),
	)
	r.Equal(1, cbCalled)
}
