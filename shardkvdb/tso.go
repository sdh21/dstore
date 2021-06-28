package shardkvdb

import (
    "errors"
    "github.com/sdh21/dstore/kvstore"
    "github.com/sdh21/dstore/utils"
    "sync"
    "time"
)

var (
    ErrTSBucketEmpty = errors.New("timestamp's bucket is empty")
)

// TimestampService provides (globally) monotonically increasing timestamp for each transaction
type TimestampService struct {
    db       *kvstore.KeyValueDB
    tsValid  bool
    tsCur    int64
    tsMax    int64
    lock     sync.Mutex

    cacheTSCount int64
}

func NewTimestampService(db *kvstore.KeyValueDB) *TimestampService {
    tso := &TimestampService{
        db:    db,
        tsCur: 0,
        tsMax: 0,
    }

    return tso
}

func (tso *TimestampService) GetTimestamp() (int64, error) {
    tso.lock.Lock()
    defer tso.lock.Unlock()
    ts := tso.tsCur
    if ts > tso.tsMax {
        return -1, ErrTSBucketEmpty
    }
    tso.tsCur++
    return tso.tsCur, nil
}

func (tso *TimestampService) CacheTimestamp() error {

    tso.lock.Lock()
    cacheTo := tso.tsMax + tso.cacheTSCount
    tso.lock.Unlock()

    reply, err := tso.db.BatchSubmit(nil, &kvstore.BatchSubmitArgs{Wrapper:
        &kvstore.OpWrapper{
        Count:        1,
        ForwarderId:  0,
        WrapperId:    0,
        Transactions: []*kvstore.Transaction{&kvstore.Transaction{
            ClientId:             "",
            TransactionId:        0,
            CollectionId:         "tso",
            Ops:                  []*kvstore.AnyOp{
                kvstore.OpCheck(),
                kvstore.OpMapStore([]string{}, "cached-ts", cacheTo),
            },
            TableVersionExpected: -1,
            TimeStamp:            time.Now().Unix(),
        }},
    }})

    if err != nil || !reply.OK || reply. {
        return reply.Result.TransactionResults[0].Status
    }


    tso.lock.Lock()
    if cacheTo > tso.tsMax {
        tso.tsMax = cacheTo
    } else {
        utils.Warning("tso tsMax goes backward")
    }
    tso.lock.Unlock()
}

