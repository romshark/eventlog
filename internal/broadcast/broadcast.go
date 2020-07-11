package broadcast

import (
	"sync"
)

type Broadcast struct {
	lock      sync.Mutex
	idCounter uint64
	subs      map[uint64]chan<- uint64
}

func New() *Broadcast {
	return &Broadcast{
		subs: map[uint64]chan<- uint64{},
	}
}

func (b *Broadcast) Broadcast(value uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, c := range b.subs {
		select {
		case c <- value:
		default:
		}
	}
}

// Subscribe creates a new subscription triggering the returned
// channel in case of a broadcast which must eventually be closed
// by calling unsub
func (b *Broadcast) Subscribe(channel chan<- uint64) (unsub func()) {
	if channel == nil {
		return nil
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	id := b.idCounter
	b.idCounter++
	b.subs[id] = channel

	unsub = func() {
		b.lock.Lock()
		defer b.lock.Unlock()

		if c, ok := b.subs[id]; ok {
			close(c)
			delete(b.subs, id)
		}
	}
	return
}

// Close closes all subscriptions and resets the broadcast
func (b *Broadcast) Close() {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, c := range b.subs {
		close(c)
	}
	b.subs = map[uint64]chan<- uint64{}
}
