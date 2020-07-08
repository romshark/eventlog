package broadcast_test

import (
	"testing"

	"github.com/romshark/eventlog/internal/broadcast"
	"github.com/stretchr/testify/require"
)

func TestSubscribe(t *testing.T) {
	b := broadcast.New()

	c1 := make(chan uint64, 1)
	c2 := make(chan uint64, 1)

	close := b.Subscribe(c1)
	defer close()

	close = b.Subscribe(c2)
	defer close()

	expected := uint64(42)
	b.Broadcast(expected)

	require.Equal(t, expected, <-c1)
	require.Equal(t, expected, <-c2)

	expected = uint64(56)
	b.Broadcast(expected)

	require.Equal(t, expected, <-c1)
	require.Equal(t, expected, <-c2)
}

func TestClose(t *testing.T) {
	b := broadcast.New()

	c1 := make(chan uint64, 1)
	close := b.Subscribe(c1)
	close()

	c2 := make(chan uint64, 1)
	close = b.Subscribe(c2)
	defer close()

	const expected = uint64(42)
	b.Broadcast(expected)

	v, ok := <-c1
	require.False(t, ok)
	require.Zero(t, v)

	v, ok = <-c2
	require.True(t, ok)
	require.Equal(t, expected, v)

}

func TestCloseAll(t *testing.T) {
	b := broadcast.New()

	c1 := make(chan uint64, 1)
	_ = b.Subscribe(c1)

	c2 := make(chan uint64, 1)
	_ = b.Subscribe(c2)

	b.Close()

	v, ok := <-c1
	require.False(t, ok)
	require.Zero(t, v)

	v, ok = <-c2
	require.False(t, ok)
	require.Zero(t, v)
}
