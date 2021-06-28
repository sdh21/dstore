package shardkvdb

import (
	"github.com/sdh21/dstore/cert"
	"sync"
	"sync/atomic"
)

// DBAccessLayer batches users' requests to alleviate sync overhead.
type DBAccessLayer struct {
	// The maximum concurrent requests sent to db.
	ConcurrentQs uint64

	ThrottleThreshold uint64

	client  *Client
	q       []*ConcurrentChannel
	qRotate uint64

	qPureRead       []*ConcurrentChannel
	qPureReadRotate int64
}

// NewDBAccessLayer creates a new db client instance with batch supported.
// ConcurrentQs indicates how many requests
// are concurrently sent to db.
func NewDBAccessLayer(concurrentQs uint64, servers []string, clientId int64, tlsConfig *cert.MutualTLSConfig) *DBAccessLayer {
	q := make([]*ConcurrentChannel, concurrentQs)
	for i, _ := range q {
		q[i] = &ConcurrentChannel{}
		q[i].reqWaiting = NewBundledRequests()
	}
	return &DBAccessLayer{
		ConcurrentQs: concurrentQs,
		client:       NewClient(servers, clientId, tlsConfig),
		q:            q,
		qRotate:      0,
	}
}

type ConcurrentChannel struct {
	mu         sync.Mutex
	busy       bool
	reqWaiting *BundledRequests
}

type BundledRequests struct {
	queuedTs  []*Transaction
	tChannels []chan *TransactionResult
}

func NewBundledRequests() *BundledRequests {
	return &BundledRequests{
		queuedTs:  make([]*Transaction, 0),
		tChannels: make([]chan *TransactionResult, 0),
	}
}

// Submit a transaction. ClientId and TransactionId can be left empty if
// the db servers' SaveProcessedResults option is not enabled.
func (db *DBAccessLayer) Submit(t *Transaction) *TransactionResult {
	q := db.q[atomic.AddUint64(&db.qRotate, 1)%db.ConcurrentQs]
	q.mu.Lock()
	c := make(chan *TransactionResult, 1)
	q.reqWaiting.queuedTs = append(q.reqWaiting.queuedTs, t)
	q.reqWaiting.tChannels = append(q.reqWaiting.tChannels, c)
	if q.busy {
		q.mu.Unlock()
		result := <-c
		return result
	} else {
		reqSent := q.reqWaiting
		q.reqWaiting = NewBundledRequests()
		q.busy = true
		q.mu.Unlock()
		reply := db.client.Submit(&BatchSubmitArgs{Transactions: db.client.CreateBundledOp(reqSent.queuedTs...)})
		if reply.OK {
			for i, ch := range reqSent.tChannels {
				ch <- reply.Result.TransactionResults[i]
			}
		} else {
			for _, ch := range reqSent.tChannels {
				ch <- nil
			}
		}
		q.mu.Lock()
		q.busy = false
		q.mu.Unlock()
		result := <-c
		return result
	}
}
