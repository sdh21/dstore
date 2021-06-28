package paxos

import (
	"testing"
)

func TestArrayQueue(t *testing.T) {
	aq := NewArrayQueue(5)
	aq.PushBack(&Instance{HighestAcN: 1})
	aq.PushBack(&Instance{HighestAcN: 2})
	aq.PushBack(&Instance{HighestAcN: 3})
	aq.PushBack(&Instance{HighestAcN: 4})
	aq.PushBack(&Instance{HighestAcN: 5})
	if aq.size != 5 {
		t.Fatalf("size")
	}
	i := int64(0)
	for i = 0; i < 5; i++ {
		ins, _ := aq.GetAt(i)
		if ins.HighestAcN != i+1 {
			t.Fatalf("value")
		}
	}
	aq.PopFront(2)
	for i = 0; i < 2; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 2; i < 5; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value2")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 6})
	for i = 0; i < 2; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 2; i < 6; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value3")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 7})
	for i = 0; i < 2; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 2; i < 7; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value4")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 8})
	for i = 0; i < 2; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 2; i < 8; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value5")
		}
	}
	aq.PopFront(2)
	for i = 0; i < 4; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 4; i < 8; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value6")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 9})
	for i = 0; i < 4; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 4; i < 9; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value6")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 10})
	for i = 0; i < 4; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 4; i < 10; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i+1 {
			t.Fatalf("value6")
		}
	}
	aq.PopFront(aq.size)
	for i = 0; i < 10; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}

}

func TestArrayQueue2(t *testing.T) {
	aq := NewArrayQueue(5)
	aq.PushBack(&Instance{HighestAcN: 0})
	aq.PushBack(&Instance{HighestAcN: 1})
	aq.PushBack(&Instance{HighestAcN: 2})
	aq.PushBack(&Instance{HighestAcN: 3})
	aq.PushBack(&Instance{HighestAcN: 4})
	aq.PushBack(&Instance{HighestAcN: 5})
	aq.PopInstancesBefore(3)

	if aq.size != 3 {
		t.Fatalf("size")
	}

	i := int64(0)
	for i = 0; i < 3; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 3; i <= 5; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i {
			t.Fatalf("value")
		}
	}

	aq.PushBack(&Instance{HighestAcN: 6})
	if aq.size != 4 {
		t.Fatalf("size")
	}
	for i = 0; i < 3; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 3; i <= 6; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i {
			t.Fatalf("value")
		}
	}
	aq.PopInstancesBefore(6)
	if aq.size != 1 {
		t.Fatalf("size")
	}
	for i = 0; i < 6; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 6; i <= 6; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i {
			t.Fatalf("value")
		}
	}
	aq.PopInstancesBefore(7)
	if aq.size != 0 {
		t.Fatalf("size")
	}
	for i = 0; i <= 6; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	aq.PushBack(&Instance{HighestAcN: 7})
	if aq.size != 1 {
		t.Fatalf("size")
	}
	for i = 0; i <= 6; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 7; i <= 7; i++ {
		ins, _ := aq.GetAt(i)
		if (ins.HighestAcN) != i {
			t.Fatalf("value")
		}
	}
}

func TestArrayQueueCreateAt(t *testing.T) {
	aq := NewArrayQueue(1)
	// front = 0, cap not enough
	aq.GetAtOrCreateAt(10)
	// 0-10 are created
	if aq.size != 11 {
		t.Fatalf("?")
	}
	if aq.GetFirstIndex() != 0 {
		t.Fatalf("?")
	}
	if aq.GetHighestIndex() != 10 {
		t.Fatalf("?")
	}

	i := int64(0)
	for i = 0; i <= 10; i++ {
		v, found := aq.GetAt(i)
		if !found {
			t.Fatalf("missing")
		}
		if v == nil {
			t.Fatalf("value nil")
		}
	}

	aq.PopFront(3)
	// aq.front != 0
	if aq.front != 3 {
		t.Fatalf("?")
	}

	if aq.size != 11-3 {
		t.Fatalf("?")
	}
	if aq.GetFirstIndex() != 3 {
		t.Fatalf("?")
	}
	if aq.GetHighestIndex() != 10 {
		t.Fatalf("?")
	}

	aq.GetAtOrCreateAt(23)
	if aq.GetFirstIndex() != 3 {
		t.Fatalf("?")
	}
	if aq.GetHighestIndex() != 23 {
		t.Fatalf("?")
	}
	if aq.size != 24-3 {
		t.Fatalf("??")
	}
	for i = 0; i < 3; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 3; i <= 23; i++ {
		v, found := aq.GetAt(i)
		if !found {
			t.Fatalf("missing")
		}
		if v == nil {
			t.Fatalf("value nil")
		}
	}
	aq.PopInstancesBefore(28)
	if aq.GetFirstIndex() != 28 {
		t.Fatalf("?")
	}
	// this is an undefined behaviour
	if aq.GetHighestIndex() != 27 {
		t.Fatalf("?")
	}
	aq.GetAtOrCreateAt(50017)
	if aq.size != 50017+1-28 {
		t.Fatalf("??")
	}
	if aq.GetFirstIndex() != 28 {
		t.Fatalf("?")
	}
	if aq.GetHighestIndex() != 50017 {
		t.Fatalf("?")
	}
	for i = 0; i < 28; i++ {
		_, found := aq.GetAt(i)
		if found {
			t.Fatalf("?")
		}
	}
	for i = 28; i <= 50017; i++ {
		v, found := aq.GetAt(i)
		if !found {
			t.Fatalf("missing")
		}
		if v == nil {
			t.Fatalf("value nil")
		}
	}
}

func TestArrayQueueSetOffset(t *testing.T) {
	aq := NewArrayQueue(1)
	aq.PopInstancesBefore(10)
	if aq.GetFirstIndex() != 10 {
		t.Fatalf("first index wrong")
	}
	_, ok := aq.GetAtOrCreateAt(10)
	if !ok {
		t.Fatalf("first missing")
	}
	_, ok = aq.GetAtOrCreateAt(11)
	if !ok {
		t.Fatalf("second missing")
	}
}
