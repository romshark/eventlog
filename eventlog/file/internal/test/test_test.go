package test_test

import (
	"io"
	"testing"

	"github.com/romshark/eventlog/eventlog/file/internal/test"

	"github.com/stretchr/testify/require"
)

func TestFakeSrc(t *testing.T) {
	r := require.New(t)
	f := test.FakeSrc("0123456789")

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
