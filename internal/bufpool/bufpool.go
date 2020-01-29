package bufpool

import (
	"bytes"
	"fmt"
	"sync"
)

// Buffer is a buffer instance
type Buffer struct {
	bytes.Buffer
	src *Pool
}

// Release releases the buffer putting it back to the pool
func (b *Buffer) Release() {
	if b.Cap() != b.src.bufLen {
		// The capacity of buffers is expected to remain
		// constant in normal conditions. A change in buffer
		// capacity may indicate internal inconsistencies
		panic(fmt.Errorf(
			"buffer cap changed unexpectedly (%d / %d)",
			b.Cap(),
			b.src.bufLen,
		))
	}
	b.Buffer.Reset()
	b.src.pool.Put(b)
}

// Pool is a buffer pool
type Pool struct {
	bufLen int
	pool   sync.Pool
}

// NewPool returns a new buffer pool
//
// NOTE: The given buffer length will
// automatically be set to 64 if it's smaller
func NewPool(bufLen int) *Pool {
	if bufLen < 64 {
		bufLen = 64
	}
	p := &Pool{
		bufLen: bufLen,
	}
	p.pool = sync.Pool{
		New: func() interface{} {
			b := &Buffer{src: p}
			b.Buffer.Grow(bufLen)
			return b
		},
	}
	return p
}

// Get returns a Buffer
//
// WARNING: the buffer needs to be released as soon as it's no longer needed.
// Aliases of the buffer's internal byte-slice should not be used after
// the buffer is released
func (p *Pool) Get() *Buffer {
	return p.pool.Get().(*Buffer)
}
