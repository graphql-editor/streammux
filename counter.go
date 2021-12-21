package streammux

import (
	"sync/atomic"
	"time"
)

type counter struct {
	state  uint32
	bitset [1024]uint64
	signal chan struct{}
}

func (c *counter) idx(v uint16) uint16 {
	// lower 10 bits as index into a bitset
	// to keep memory access as spread as possible
	return v & 0x3FF
}

func (c *counter) valueMask(v uint16) uint64 {
	// higher 6 bits as a value mask
	return 1 << ((v & 0xFC00) >> 10)
}

func (c *counter) idxAndBit(state uint16) (uint16, uint64) {
	idx := c.idx(state)
	return idx, c.valueMask(state)
}

func (c *counter) inUse(idx uint16, bit uint64) (uint64, bool) {
	bt := atomic.LoadUint64(&c.bitset[idx])
	return bt, bt&bit != 0
}

func (c *counter) cas(idx uint16, old, nw uint64) bool {
	return atomic.CompareAndSwapUint64(&c.bitset[idx], old, nw)
}

func (c *counter) setBit(idx uint16, bit, bt uint64) bool {
	return c.cas(idx, bt, bt+bit)
}

func (c *counter) clearBit(idx uint16, bit, bt uint64) bool {
	return c.cas(idx, bt, bt-bit)
}

func (c *counter) acquire(state uint16) bool {
	idx, bit := c.idxAndBit(state)
	bt, inUse := c.inUse(idx, bit)
	for !inUse && !c.setBit(idx, bit, bt) {
		bt, inUse = c.inUse(idx, bit)
	}
	return !inUse
}

func (c *counter) release(state uint16) {
	idx, bit := c.idxAndBit(state)
	bt, inUse := c.inUse(idx, bit)
	for inUse && !c.clearBit(idx, bit, bt) {
		bt, inUse = c.inUse(idx, bit)
	}
	select {
	case c.signal <- struct{}{}:
	default:
	}
}

func (c *counter) increaseState(counterCap uint16) uint16 {
	state := atomic.AddUint32(&c.state, 1)
	for state >= uint32(counterCap) {
		atomic.CompareAndSwapUint32(&c.state, state, 0)
		state = atomic.AddUint32(&c.state, 1)
	}
	return uint16(state)
}

func minUint16(a, b uint16) uint16 {
	if a < b {
		return a
	}
	return b
}

func (c *counter) scanWithCap(counterCap, seekCap uint16) (uint16, bool) {
	var state uint16
	var ok bool
	for i := 0; i < int(seekCap) && !ok; i++ {
		state = c.increaseState(counterCap)
		ok = c.acquire(state)
	}
	return state, ok
}

func (c *counter) nextVal(counterCap uint16) uint16 {
	seekCap := minUint16(counterCap, 64)
	// semi active spin on state with optional wake up signal from release
	for {
		state, ok := c.scanWithCap(counterCap, seekCap)
		if ok {
			return state
		}
		t := time.NewTimer(time.Microsecond * 100)
		select {
		case <-c.signal:
		case <-t.C:
		}
	}
}
