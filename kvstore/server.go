package kvstore

import (
	"context"
	"errors"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/storage"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type DiskStorage struct {
	storage      *storage.Storage
	storageStats *storage.BlockStatistics
	mapLock      sync.Mutex
	key2block    map[string][]*storage.FileBlockInfo
	folder       string
}

func (dsk *DiskStorage) Write(key string, size int64, value []byte) error {
	blocks, err := dsk.storage.CreateLogFile(size, value, dsk.storageStats)
	if err != nil {
		return err
	}
	dsk.mapLock.Lock()
	_, found := dsk.key2block[key]
	if found {
		dsk.mapLock.Unlock()
		return errors.New("kvstore Write, key already exists")
	}
	dsk.key2block[key] = blocks
	dsk.mapLock.Unlock()
	return nil
}

func (dsk *DiskStorage) Delete(key string) error {
	dsk.mapLock.Lock()
	blocks := dsk.key2block[key]
	delete(dsk.key2block, key)
	dsk.mapLock.Unlock()
	if blocks == nil {
		return errors.New("trying to release a non-existing file")
	}
	err := dsk.storage.DeleteLogFile(blocks, dsk.storageStats)
	return err
}

type KeyValueDB struct {
	// db socket serving incoming requests
	l         net.Listener
	server    *grpc.Server
	address   string
	addresses []string

	closed     int32
	unreliable bool // for testing
	px         *paxos.Paxos

	decideChannel chan paxos.DecideEvent

	mu    sync.Mutex
	state *DBState
	// proposal id that includes nil proposals
	lastProposalId int64

	requestsBlockList     map[int64]*waitProposal
	requestsBlockListLock sync.Mutex
	waitingCount          int64

	checkpoint *checkpointInst

	// Indicates whether or not to discard results of processed transactions.
	// Saving results is somewhat inefficient and consumes lots of memory, so
	// if you are not debugging, set it to true and use version control instead.
	// With this set to false, you can find the result back if the reply packet is missing.
	// If you choose to do so, you need to provide a unique client id,
	// and make sure each request's transaction id is monotonically increasing.
	discardResults bool

	uniqueIdPart int64

	UnimplementedKVStoreRPCServer
}

type DBConfig struct {
	StorageFolder    string
	PaxosFolder      string
	StorageBlockSize uint32
	AllocationUnit   uint32
	PaxosConfig      *paxos.Config
	// db address is different from paxos address
	// in order to allow different tls settings
	DBAddress string
	// All DB servers, not used
	Addresses []string
	Listener  net.Listener

	TLS *cert.MutualTLSConfig

	Checkpoint CheckpointConfig

	SaveProcessedResults bool
}

func DefaultConfig() *DBConfig {
	config := &DBConfig{}
	config.StorageBlockSize = 64 * 1024 * 1024
	config.AllocationUnit = 4 * 1024
	config.TLS = cert.TestTlsConfig()
	config.PaxosConfig = &paxos.Config{
		Peers:                        nil,
		Me:                           0,
		Storage:                      nil,
		Timeout:                      3000 * time.Millisecond,
		RandomBackoffMax:             3000,
		HeartbeatInterval:            1000,
		LeaderTimeoutRTTs:            10,
		LeaderTimeout:                10000,
		DecideChannel:                make(chan paxos.DecideEvent, 100),
		Listener:                     nil,
		AdditionalTimeoutPer100Holes: 100 * time.Millisecond,
		TLS:                          cert.TestTlsConfig(),
	}
	config.Checkpoint.FullCheckpointAfterNSeconds = 120
	config.Checkpoint.IncrementalCheckpointEveryNLog = 10000
	config.Checkpoint.FullCheckpointEveryNLog = 1000000
	config.SaveProcessedResults = true
	return config
}

const MaxGRPCMsgSize = 512 * 1024 * 1024

func NewServer(config *DBConfig) (*KeyValueDB, error) {

	db := &KeyValueDB{}
	px, err := initializePaxos(config)
	if err != nil {
		return nil, err
	}
	db.px = px
	err = db.initializeCheckPoint(config)
	if err != nil {
		return nil, err
	}

	serverTLSConfig, _, err := cert.LoadMutualTLSConfig(config.TLS)
	if err != nil {
		return nil, err
	}

	db.state = NewDBState()
	db.decideChannel = config.PaxosConfig.DecideChannel
	db.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig)),
		grpc.MaxRecvMsgSize(MaxGRPCMsgSize), grpc.MaxSendMsgSize(MaxGRPCMsgSize),
		grpc.MaxConcurrentStreams(20000))
	db.address = config.DBAddress
	db.addresses = config.Addresses
	db.requestsBlockList = map[int64]*waitProposal{}
	db.discardResults = !config.SaveProcessedResults
	db.l = config.Listener
	return db, nil
}

func (db *KeyValueDB) BatchSubmit(ctx context.Context, args *BatchSubmitArgs) (*BatchSubmitReply, error) {
	reply := &BatchSubmitReply{}

	if len(args.Transactions) == 0 {
		return nil, errors.New("empty transaction")
	}

	args.BatchSubmitId = int64(db.px.GetMe()) | atomic.AddInt64(&db.uniqueIdPart, 1)<<10

	// Indicate that someone is waiting and we should not
	// discard apply results.
	atomic.AddInt64(&db.waitingCount, 1)
	defer func() {
		atomic.AddInt64(&db.waitingCount, -1)
	}()

	proposeValueResult := db.px.ProposeValue(args, func(i interface{}, i2 interface{}) bool {
		if i == nil || i2 == nil {
			return false
		}
		return i.(*BatchSubmitArgs).BatchSubmitId == i2.(*BatchSubmitArgs).BatchSubmitId
	})

	if !proposeValueResult.Succeeded {
		// redirect requests to leader
		reply.OK = false
		if proposeValueResult.LeaderHint != -1 {
			reply.TalkTo = "" // db.addresses[proposeValueResult.LeaderHint]
		}
		if proposeValueResult.Reason != paxos.FailReasonProposerNotLeader {
			// client should not talk to us for a period of time
			// we might be an old leader in a network partition
			reply.InternalError = true
		}
		//logger.Info("KeyValueDB %v propose failed reason: %v, value: %v", db.address, proposeValueResult.Reason, opWrapper)
		return reply, nil
	} else {
		w := &waitProposal{
			proposalId: proposeValueResult.InstanceId,
			waitChan:   make(chan bool),
		}
		proposalId := proposeValueResult.InstanceId
		//logger.Info("KeyValueDB %v propose success instanceId: %v, value: %v", db.address, w.proposalId, opWrapper)
		db.requestsBlockListLock.Lock()
		// wait for the proposal to be applied
		applyResult, found := db.requestsBlockList[proposalId]
		if found {
			// already there
			reply.TransactionResults = applyResult.result.TransactionResults
			if reply.TransactionResults == nil {
				log.Fatalf("nil result!!")
			}
			delete(db.requestsBlockList, proposalId)
			db.requestsBlockListLock.Unlock()
		} else {
			// waiting for result
			db.requestsBlockList[proposalId] = w
			db.requestsBlockListLock.Unlock()
			_ = <-w.waitChan
			db.requestsBlockListLock.Lock()
			delete(db.requestsBlockList, proposalId)
			db.requestsBlockListLock.Unlock()
			if w.result == nil {
				log.Fatalf("nil result!!!!!!!!!")
			}
			reply.TransactionResults = w.result.TransactionResults
		}
		close(w.waitChan)
		if reply.TransactionResults == nil {
			log.Fatalf("nil result!!")
		}
		reply.OK = true
		return reply, nil
	}
}

type waitProposal struct {
	proposalId int64
	result     *BatchSubmitReply
	waitChan   chan bool
}

func (db *KeyValueDB) waitDecide() {
	go func() {
		for {
			event, ok := <-db.decideChannel
			if !ok {
				break
			}
			// we do not have shards and TSO service;
			// use InstanceId to provide TxnTS
			txns := event.DecidedValue.(*BatchSubmitArgs).Transactions
			if len(txns) >= 64 {
				panic("too many transactions")
			}
			for ti := range txns {
				if txns[ti].TxnTimestamp == -1 {
					txns[ti].TxnTimestamp = event.InstanceId<<6 | int64(ti)
				}
			}
			db.checkpoint.eventChannel <- event
			atomic.StoreInt64(&db.lastProposalId, event.InstanceId)
			if event.DecidedValue == nil {
				continue
			}
			db.requestsBlockListLock.Lock()
			applyResult := db.applyTransactions(db.state, event.DecidedValue.(*BatchSubmitArgs), event.InstanceId)
			if atomic.LoadInt64(&db.waitingCount) <= 0 {
				// just discard the applyResult.
				// also, we can reset requestsBlockList
				// since no one is actually waiting.
				if len(db.requestsBlockList) > 0 {
					db.requestsBlockList = map[int64]*waitProposal{}
				}
				db.requestsBlockListLock.Unlock()
				continue
			}
			ws, found := db.requestsBlockList[event.InstanceId]
			if found {
				// someone is waiting
				ws.result = applyResult
				ws.waitChan <- true
			} else {
				// no one is waiting, just put there
				db.requestsBlockList[event.InstanceId] = &waitProposal{
					proposalId: event.InstanceId,
					result:     applyResult,
					waitChan:   nil,
				}
			}
			db.requestsBlockListLock.Unlock()
		}
	}()
}

// Close gracefully shutdowns server
func (db *KeyValueDB) Close() {
	atomic.StoreInt32(&db.closed, 1)
	db.px.Close()
	db.server.Stop()
	close(db.decideChannel)
}

func (db *KeyValueDB) StartServer() {
	db.px.StartServer()

	RegisterKVStoreRPCServer(db.server, db)

	if db.l == nil {
		l, err := net.Listen("tcp", db.address)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
		db.l = l
	}

	go func() {
		err := db.server.Serve(db.l)
		if err != nil {
			utils.Error("serve err: %v", err)
		}
	}()

	db.waitDecide()
	db.startCheckpointRoutine()
}
