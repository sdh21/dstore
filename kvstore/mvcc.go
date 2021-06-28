package kvstore

import (
	"log"
	"sync"
)

type MVCCStore struct {
	// documentID -> value
	kv   map[string]*DocCell
	lock sync.Mutex
}

type DocCell struct {
	// multiple-version committed values
	values []DataCell
	lock   sync.Mutex

	// the maximum txn that reads the newest value
	maxTSReadNewest int64

	txnLock     int32
	toWrite     DataCell
	committedTS []int64
	abortedTS   []int64
}

type DataCell struct {
	data interface{}
	ts   int64
}

func NewMVCCStore() *MVCCStore {
	store := &MVCCStore{}
	store.kv = map[string]*DocCell{}
	return store
}

func NewDocCell() *DocCell {
	cell := &DocCell{
		values:      []DataCell{},
		txnLock:     0,
		toWrite:     DataCell{},
		committedTS: []int64{},
		abortedTS:   []int64{},
	}
	return cell
}

func (store *MVCCStore) CreateDoc(key string) (*DocCell, bool) {
	store.lock.Lock()
	defer store.lock.Unlock()
	cell, found := store.kv[key]
	if found {
		return nil, false
	}
	cell = NewDocCell()
	store.kv[key] = cell
	return cell, true
}

func (store *MVCCStore) GetDoc(key string) (*DocCell, bool) {
	store.lock.Lock()
	defer store.lock.Unlock()
	cell, found := store.kv[key]
	if !found {
		return nil, false
	}
	return cell, true
}

func (cell *DocCell) PrepareWrite(commitTS int64, value interface{}) bool {
	cell.lock.Lock()
	defer cell.lock.Unlock()

	if len(cell.values) > 0 && cell.values[len(cell.values)-1].ts >= commitTS {
		return false
	}
	if cell.txnLock == 1 {
		return false
	}

	if cell.maxTSReadNewest > commitTS {
		// this txn will cause a newer read
		// to violate serializability
		return false
	}

	cell.txnLock = 1
	cell.toWrite.data = value
	cell.toWrite.ts = commitTS

	return true
}

func (cell *DocCell) CommitWrite(commitTS int64) bool {
	cell.lock.Lock()
	defer cell.lock.Unlock()

	if commitTS != cell.toWrite.ts || cell.txnLock == 0 {
		log.Println("should not happen")
		return false
	}

	cell.committedTS = append(cell.committedTS, commitTS)

	cell.values = append(cell.values, DataCell{
		data: cell.toWrite.data,
		ts:   commitTS,
	})

	cell.txnLock = 0
	cell.toWrite.ts = -1
	cell.toWrite.data = nil

	return true
}

func (cell *DocCell) AbortWrite(commitTS int64) bool {
	cell.lock.Lock()
	defer cell.lock.Unlock()

	if commitTS != cell.toWrite.ts {
		return false
	}
	if cell.txnLock == 0 {
		return false
	}

	cell.abortedTS = append(cell.abortedTS, commitTS)

	cell.txnLock = 0
	cell.toWrite.ts = -1
	cell.toWrite.data = nil

	return true
}

func (cell *DocCell) binarySearchNoLock(ts int64) *DataCell {
	if len(cell.values) == 0 {
		return nil
	}

	if ts == -1 {
		return &cell.values[len(cell.values)-1]
	}

	// binary search

	l := 0
	r := len(cell.values) - 1
	for l < r {
		mid := l + (r-l)/2
		if cell.values[mid].ts <= ts {
			r = mid
		} else {
			l = mid + 1
		}
	}
	if cell.values[r].ts > ts {
		return nil
	}
	return &cell.values[r]
}

func (cell *DocCell) Read(commitTS int64) (*DataCell, bool) {
	cell.lock.Lock()
	defer cell.lock.Unlock()

	// a txn has already acquired the 2pc lock
	// and is going to commit
	// so abort this read
	if cell.toWrite.ts <= commitTS && cell.txnLock == 1 {
		return nil, false
	}

	dataCell := cell.binarySearchNoLock(commitTS)

	if dataCell.ts < commitTS && commitTS > cell.maxTSReadNewest {
		cell.maxTSReadNewest = commitTS
	}
	return dataCell, true
}
