package paxos

import (
	"fmt"
	"github.com/sdh21/dstore/utils"
	"log"
	"sync"
)

// a simple data structure to store paxos instances

type ArrayQueue struct {
	front  int64
	back   int64
	size   int64
	cap    int64
	offset int64
	data   []*Instance
	lock   sync.RWMutex
}

// use this method to get freed instance id
func (aq *ArrayQueue) GetFirstIndex() int64 {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	return aq.offset
}

func (aq *ArrayQueue) GetAt(index int64) (*Instance, bool) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	index -= aq.offset
	if index < 0 || index >= aq.size {
		return nil, false
	}
	return aq.data[(aq.front+index)%aq.cap], true
}

func (aq *ArrayQueue) GetHighestIndex() int64 {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.offset + aq.size - 1
}

func (aq *ArrayQueue) GetAtOrCreateAt(instanceId int64) (*Instance, bool) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	return aq.getAtOrCreateAtNoLock(instanceId)
}

func (aq *ArrayQueue) getAtOrCreateAtNoLock(instanceId int64) (*Instance, bool) {
	index := instanceId - aq.offset
	if index < 0 {
		// already released instances
		return nil, false
	}
	if index < aq.size {
		// found
		inst := aq.data[(aq.front+index)%aq.cap]
		if inst == nil {
			fmt.Printf("dump info: %v\n", aq)
			log.Fatalf("found inst should not be nil, check array_queue")
		}
		return inst, true
	}
	// create
	oriBack := aq.back
	oriCap := aq.cap
	oriSize := aq.size
	utils.Debug("%v %v %v\n", oriBack, oriCap, oriSize)
	from := (aq.back + 1) % aq.cap
	sizeNeeded := index - aq.size + 1
	reserveNeeded := sizeNeeded > aq.cap-aq.size
	if reserveNeeded {
		from = aq.size
		aq.reserve((aq.cap + sizeNeeded) + (aq.cap+sizeNeeded)/2 + 1)
		aq.back = index // because aq.front=0
		aq.size = index + 1
	} else {
		aq.back = (aq.front + index) % aq.cap
		aq.size = index + 1
		if aq.size > aq.cap {
			log.Fatalf("reserve?")
		}
	}
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	// initialize to aq.back
	cnt := int64(0)
	for i := from; ; i = (i + 1) % aq.cap {
		inst := new(Instance)
		inst.HighestAcN = -1
		inst.HighestAcV = make([]byte, 0)
		// deliberately made complex
		inst.InstanceId = instanceId - sizeNeeded + cnt + 1
		if aq.data[i] != nil {
			log.Fatalf("wrong array queue impl")
		}
		aq.data[i] = inst
		cnt++
		if i == aq.back {
			break
		}
	}
	if cnt != sizeNeeded {
		log.Fatalf("wrong cnt in array queue")
	}

	return aq.data[aq.back], true
}

//  Deprecated: item.InstanceId=Highest+1 is not atomic,
//  Use PushBackEmpty instead
func (aq *ArrayQueue) PushBack(item *Instance) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	if aq.size == aq.cap {
		aq.reserve(aq.cap + aq.cap/2 + 1)
	}
	aq.back = (aq.back + 1) % aq.cap
	aq.data[aq.back] = item
	aq.size++
}

func (aq *ArrayQueue) PushBackEmpty() *Instance {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	if aq.size == aq.cap {
		aq.reserve(aq.cap + aq.cap/2 + 1)
	}
	aq.back = (aq.back + 1) % aq.cap
	inst := new(Instance)
	inst.HighestAcN = -1
	inst.HighestAcV = make([]byte, 0)
	inst.InstanceId = aq.size + aq.offset
	aq.data[aq.back] = inst
	aq.size++
	return inst
}

func (aq *ArrayQueue) PopFront(size int64) []*Instance {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	if size <= 0 {
		return nil
	}
	if size > aq.size {
		size = aq.size
	}
	aq.size -= size
	oldFront := aq.front
	aq.front = (aq.front + size) % aq.cap
	aq.offset += size
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	result := make([]*Instance, 0, size)

	for i := int64(0); i < size; i++ {
		idx := (oldFront + i) % aq.cap
		result = append(result, aq.data[idx])
		aq.data[idx] = nil
	}

	if aq.size < 0 {
		log.Fatalf("size < 0")
	}

	return result

}

func (aq *ArrayQueue) PopInstancesBefore(index int64) []*Instance {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	size := index - aq.offset
	if size <= 0 {
		return nil
	}
	if size > aq.size {
		size = aq.size
	}
	oldFront := aq.front
	aq.size -= size
	aq.front = (aq.front + size) % aq.cap
	aq.offset += index - aq.offset
	if aq.size < 0 {
		log.Fatalf("size < 0")
	}
	result := make([]*Instance, 0, size)
	for i := int64(0); i < size; i++ {
		idx := (oldFront + i) % aq.cap
		result = append(result, aq.data[idx])
		aq.data[idx] = nil
	}

	if aq.size < 0 {
		log.Fatalf("size < 0")
	}

	return result
}

func (aq *ArrayQueue) Reserve(capacity int64) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	aq.reserve(capacity)
}

func (aq *ArrayQueue) Size() int64 {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.size
}

func (aq *ArrayQueue) reserve(cap int64) {
	if cap > aq.cap {
		buffer := make([]*Instance, cap, cap)
		j := aq.front
		for i := int64(0); i < aq.size; i++ {
			buffer[i] = aq.data[j]
			j = (j + 1) % aq.cap
		}
		aq.cap = cap
		aq.data = buffer
		aq.front = 0
		aq.back = aq.size - 1
	}
}

func NewArrayQueue(capacity int64) *ArrayQueue {
	return &ArrayQueue{
		front:  0,
		back:   capacity - 1,
		size:   0,
		cap:    capacity,
		offset: 0,
		data:   make([]*Instance, capacity),
	}
}
