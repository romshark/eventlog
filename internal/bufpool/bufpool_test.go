package bufpool_test

import (
	"testing"

	"github.com/romshark/eventlog/internal/bufpool"

	"github.com/stretchr/testify/require"
)

func TestAcquireRelease(t *testing.T) {
	const l = 1024
	p := bufpool.NewPool(l)
	b := p.Get()
	require.Equal(t, l, b.Cap())
	require.Equal(t, 0, b.Len())
	b.Release()
}

func TestMinSize(t *testing.T) {
	const l = 1
	p := bufpool.NewPool(l)
	b := p.Get()
	require.Equal(t, 64, b.Cap())
	require.Equal(t, 0, b.Len())
	b.Release()
}

func TestGrowingBufferPanic(t *testing.T) {
	const l = 1
	p := bufpool.NewPool(l)
	b := p.Get()
	require.Equal(t, 64, b.Cap())
	require.Equal(t, 0, b.Len())
	b.Grow(1024)
	require.Panics(t, func() { b.Release() })
}
