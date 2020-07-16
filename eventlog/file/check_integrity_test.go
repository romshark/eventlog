package file_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/romshark/eventlog/eventlog"
	"github.com/romshark/eventlog/eventlog/file"
	"github.com/stretchr/testify/require"
)

func TestCheckIntegrity(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(9999999999),   // timestamp
		validJson2().xxHash,  // checksum
		validJson2().length,  // payload length
		validJson2().payload, // payload
	)

	cbCalled := 0
	r := require.New(t)
	r.NoError(file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			switch cbCalled {
			case 1:
				r.Equal(uint64(8888888888), timestamp)
				r.Equal(string(validJson().payload), string(payload))
				r.Equal(int64(4), offset)
			case 2:
				r.Equal(uint64(9999999999), timestamp)
				r.Equal(string(validJson2().payload), string(payload))
				r.Equal(int64(31), offset)
			}
			return nil
		},
	))
	r.Equal(2, cbCalled)
}

func TestCheckIntegrityUnsupportedVersion(t *testing.T) {
	f := compose(uint32(file.SupportedProtoVersion + 1))
	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal("unsupported file version (3)", err.Error())
	r.Zero(cbCalled)
}

func TestCheckIntegrityInvalidTimestamps(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(9999999999),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(8888888888),  // timestamp (invalid!)
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		"invalid timestamp (8888888888) at 31 "+
			"greater than previous (9999999999)",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityInvalidJSONPayload(t *testing.T) {
	for _, t1 := range []string{
		`{     }`,
		`["array", "is", "illegal"]`,
		`   42   `,
		`"foo   "`,
		`null    `,
		`false   `,
		`{x:"syntax error"}`,
	} {
		t.Run(t1, func(t *testing.T) {
			f := compose(
				// header
				uint32(file.SupportedProtoVersion),
				// first entry
				uint64(8888888888),  // timestamp
				validJson().xxHash,  // checksum
				validJson().length,  // payload length
				validJson().payload, // payload
				// second entry
				uint64(9999999999),               // timestamp
				uint64(xxhash.Sum64([]byte(t1))), // checksum
				uint32(len(t1)),                  // payload length
				[]byte(t1),                       // payload (invalid!)
			)

			cbCalled := 0
			r := require.New(t)
			err := file.CheckIntegrity(
				context.Background(),
				f,
				nil,
				func(timestamp uint64, payload []byte, offset int64) error {
					cbCalled++
					return nil
				},
			)

			r.Error(err)
			r.True(errors.Is(err, eventlog.ErrInvalidPayload))
			r.True(strings.HasPrefix(
				err.Error(),
				`invalid payload 31:`,
			))
			r.Equal(1, cbCalled)
		})
	}
}

func TestCheckIntegrityPayloadLengthTooSmall(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(9999999999),           // timestamp
		validJson().xxHash,           // checksum
		uint32(file.MinPayloadLen-1), // payload length (invalid!)
		validJson().payload,          // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(errors.Is(err, eventlog.ErrInvalidOffset))
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadLengthTooLarge(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(9999999999),           // timestamp
		validJson().xxHash,           // checksum
		uint32(file.MaxPayloadLen+1), // payload length (invalid!)
		validJson().payload,          // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(errors.Is(err, eventlog.ErrInvalidOffset))
	r.Equal(
		"reading entry at offset 31: invalid offset",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityPayloadTooSmall(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(9999999999),  // timestamp
		validJson().xxHash,  // checksum
		validJson2().length, // payload length
		validJson().payload, // payload (too small!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		"reading entry at offset 31: "+
			"reading payload (expected: 17; read: 7)",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMismatchingChecksum(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(9999999999),  // timestamp
		uint64(0),           // checksum (mismatching!)
		validJson().length,  // payload length
		validJson().payload, // payload
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.True(errors.Is(err, eventlog.ErrInvalidOffset))
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedTimestamp(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		[]byte{0, 0}, // timestamp (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		"reading entry at offset 31: "+
			"reading timestamp (expected: 8; read: 2)",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedChecksum(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(8888888888), // timestamp
		[]byte{0, 0},       // checksum (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		"reading entry at offset 31: "+
			"reading payload hash (expected: 8; read: 2)",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func TestCheckIntegrityMalformedPayloadLength(t *testing.T) {
	f := compose(
		// header
		uint32(file.SupportedProtoVersion),
		// first entry
		uint64(8888888888),  // timestamp
		validJson().xxHash,  // checksum
		validJson().length,  // payload length
		validJson().payload, // payload
		// second entry
		uint64(8888888888), // timestamp
		validJson().xxHash, // checksum
		[]byte{0, 0},       // payload length (malformed!)
	)

	cbCalled := 0
	err := file.CheckIntegrity(
		context.Background(),
		f,
		nil,
		func(timestamp uint64, payload []byte, offset int64) error {
			cbCalled++
			return nil
		},
	)
	r := require.New(t)
	r.Error(err)
	r.Equal(
		"reading entry at offset 31: "+
			"reading payload length (expected: 4; read: 2)",
		err.Error(),
	)
	r.Equal(1, cbCalled)
}

func compose(parts ...interface{}) fake {
	b := new(bytes.Buffer)
	for _, p := range parts {
		switch v := p.(type) {
		case uint32:
			buf := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, v)
			_, _ = b.Write(buf)
		case uint64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, v)
			_, _ = b.Write(buf)
		case []byte:
			_, _ = b.Write(v)
		default:
			panic(fmt.Errorf("unsupported part type"))
		}
	}
	return fake(b.Bytes())
}

type fake []byte

func (f fake) ReadAt(buf []byte, offset int64) (read int, err error) {
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

func TestFakeRead(t *testing.T) {
	r := require.New(t)
	f := fake("0123456789")

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

func validJson() (v struct {
	xxHash  uint64
	length  uint32
	payload []byte
}) {
	const s = `{"x":0}`
	v.payload = []byte(s)
	v.length = uint32(len(s))
	v.xxHash = uint64(xxhash.Sum64([]byte(s)))
	return
}

func validJson2() (v struct {
	xxHash  uint64
	length  uint32
	payload []byte
}) {
	const s = `{"medium":"size"}`
	v.payload = []byte(s)
	v.length = uint32(len(s))
	v.xxHash = uint64(xxhash.Sum64([]byte(s)))
	return
}
