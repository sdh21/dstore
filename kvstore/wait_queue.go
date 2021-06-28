package kvstore

import (
	"sync/atomic"
	"unsafe"
)

type mcsSpinlock struct {
	next   *mcsSpinlock
	locked int32
}

func (s *mcsSpinlock) Lock() {
	node := &mcsSpinlock{
		next:   nil,
		locked: 0,
	}

	prev := atomic.SwapPointer(&node, s)
}
