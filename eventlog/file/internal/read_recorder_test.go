package internal_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func NewReadRecorder(t *testing.T, e ...ExpectedRead) *ReadRecorder {
	return &ReadRecorder{t, e, 0}
}

type ReadRecorder struct {
	t       *testing.T
	expect  []ExpectedRead
	current int
}

func (r *ReadRecorder) ReadAt(buffer []byte, offset int64) (int, error) {
	expected := r.expect[r.current]
	r.current++
	require.Equal(r.t, expected.Offset, offset)
	l := len(expected.Write)
	require.GreaterOrEqual(r.t, len(buffer), l)
	copy(buffer[:l], expected.Write)
	return l, nil
}

type ExpectedRead struct {
	Offset int64
	Write  []byte
}
