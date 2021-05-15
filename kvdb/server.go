package kvdb

import (
	"context"
	"errors"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/storage"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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
		return errors.New("kvdb Write, key already exists")
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

	UnimplementedKeyValueDBServer
}

type DBConfig struct {
	StorageFolder    string
	PaxosFolder      string
	StorageBlockSize uint32
	AllocationUnit   uint32
	PaxosConfig      *paxos.ServerConfig
	// db address is different from paxos address
	// in order to allow different tls settings
	DBAddress string
	// All DB servers, not used
	Addresses []string
	Listener  net.Listener

	TLS *utils.MutualTLSConfig

	Checkpoint CheckpointConfig
}

func DefaultConfig() *DBConfig {
	config := &DBConfig{}
	config.StorageBlockSize = 64 * 1024 * 1024
	config.AllocationUnit = 4 * 1024
	config.TLS = utils.TestTlsConfig()
	config.PaxosConfig = &paxos.ServerConfig{
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
		TLS:                          utils.TestTlsConfig(),
	}
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

	serverTLSConfig, _, err := utils.LoadMutualTLSConfig(config.TLS)
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
	db.l = config.Listener
	return db, nil
}

func (db *KeyValueDB) BatchSubmit(ctx context.Context, args *BatchSubmitArgs) (*BatchSubmitReply, error) {
	reply := &BatchSubmitReply{}
	opWrapper := args.Wrapper
	if int64(len(opWrapper.Transactions)) != opWrapper.Count {
		reply.OK = false
		return nil, status.Error(codes.InvalidArgument, "invalid requests, count not consistent")
	}

	utils.Info("KeyValueDB %v is proposing %v", db.address, opWrapper)

	// Indicate that someone is waiting and we should not
	// discard apply results.
	atomic.AddInt64(&db.waitingCount, 1)
	defer func() {
		atomic.AddInt64(&db.waitingCount, -1)
	}()

	proposeValueResult := db.px.ProposeValue(opWrapper, func(i interface{}, i2 interface{}) bool {
		if i == nil || i2 == nil {
			return false
		}
		return i.(*OpWrapper).WrapperId == i2.(*OpWrapper).WrapperId &&
			i.(*OpWrapper).ForwarderId == i2.(*OpWrapper).ForwarderId
	})
	opWrapper = nil
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
			reply.Result = applyResult.result
			if reply.Result == nil {
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
			reply.Result = w.result
		}
		close(w.waitChan)
		if reply.Result == nil {
			log.Fatalf("nil result!!")
		}
		reply.OK = true
		return reply, nil
	}
}

type waitProposal struct {
	proposalId int64
	result     *WrapperResult
	waitChan   chan bool
}

func (db *KeyValueDB) waitDecide() {
	go func() {
		for {
			event, ok := <-db.decideChannel
			if !ok {
				break
			}
			db.checkpoint.eventChannel <- event
			atomic.StoreInt64(&db.lastProposalId, event.InstanceId)
			if event.DecidedValue == nil {
				continue
			}
			// apply OpWrapper one by one, but different transactions in
			// an OpWrapper can apply concurrently.
			db.requestsBlockListLock.Lock()
			applyResult := db.applyOpWrapper(db.state, event.DecidedValue.(*OpWrapper), event.InstanceId)
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

	RegisterKeyValueDBServer(db.server, db)

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
