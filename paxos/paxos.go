package paxos

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var grpcConcurrentStreams = 20000

type Instance struct {
	HighestAcN int64  // Na: highest accepted proposal
	HighestAcV []byte // Va: value
	Decided    bool
	DecidedV   interface{}
	lock       sync.Mutex
	InstanceId int64

	// done instances' collector needs to store additional info
	tag []string
}

// A Peer is immutable once loaded from config
// Address can change if this server is migrated
type Peer struct {
	ServerName string
	Address    string
	// This peer only needs instances >= MinimumInstanceId
	MinimumInstanceId int64
	conn              *grpc.ClientConn
	client            PaxosClient
	lock              sync.Mutex
	LastResponseTime  time.Time
	ApproximateRTT    time.Duration
}

type TestParams struct {
	TestEnabled            bool
	Unavailable            int32
	NotRespondingRPC       int32
	Unreliable             int32
	DropRequestProbability float64
	DropReplyProbability   float64
	RpcCount               int64
	LatencyLowerBound      int32
	LatencyUpperBound      int32
	SkippedPhase1Count     int
	BlockList              []int32
	// Make sure we do not send two different accept commands if we skip phase 1
	// only enabled if fastPathProposalIdCollisionDetectorEnabled=true
	fastPathProposalIdCollisionRecord sync.Map
	whichInstanceHoldProposalLock     int64
	// Detector will not release recorded instances and thus cause memory leak!
	FastPathProposalIdCollisionDetectorEnabled bool
}

type PersistentStorage interface {
	Write(key string, size int64, value []byte) error
	Delete(key string) error
}

type Paxos struct {
	me      int // which is me in peers[]
	peers   []*Peer
	timeout time.Duration

	instances *ArrayQueue

	doneLock sync.Mutex
	l        net.Listener

	storage PersistentStorage

	UnimplementedPaxosServer

	TestParams

	highestNpSeen           int64 // Np: highest seen proposal number
	highestInstanceAccepted int64

	// needed to lock highestNpSeen, highestInstanceAccepted
	varLock sync.Mutex

	// if a round of propose cannot enter fast path (i.e. skip phase 1),
	// it must acquire proposeLock to avoid collisions on global Np
	proposeLock sync.Mutex

	clientTLSConfig *tls.Config

	server *grpc.Server
	closed int32

	heartbeatInterval int
	randomBackoffMax  int
	// only a hint for when we should consider the leader lost and takeover automatically.
	// a peer can try to become leader at any time by forcefully call Propose
	leaderTimeout int
	// another hint, if we haven't heard from leader for min(max(RTT,10ms) * leaderTimeoutRTTs + heartbeatInterval, leaderTimeout)
	// we can assume we've lost the leader.
	// It is useful if we cannot determine a leaderTimeout for a high-latency network
	leaderTimeoutRTTs int
	// timeout if electing
	additionalTimeoutPer100Holes time.Duration

	waitGroup sync.WaitGroup

	// the highest decided instance id that can make decided instances sequential
	lastSequentialInstanceId int64
	decideSequentialChan     chan DecideEvent
	sequentialLock           sync.Mutex

	// proposer can directly send accept with leaderNp
	// for instances with id >= skipPrepareFrom
	leaderNp        int64
	skipPrepareFrom int64

	// If we are leader, use this id to propose to avoid holes
	// invalid if leaderNp = -1
	// nextInstanceId is actually a more strict skipPrepareFrom
	// which (in the context of fast path) does not allow duplicate proposal ids and never
	// re-collects(by preparing) instances previously proposed by us
	nextInstanceId int64

	// used to protect leaderNp, skipPrepareFrom, and nextInstanceId
	leaderInfoLock sync.Mutex

	electLock  sync.Mutex
	isElecting bool

	// (Accept Handler) instanceIdTo -> key passed to storage.Write
	// used for done instances collector
	instancesToStorageKey *ArrayQueue
	// (Prepare Handler) the last prepare log's key
	prepareNToStorageKey     string
	prepareNToStorageKeyLock sync.Mutex

	doneInstancesCleanerLock sync.Mutex
}

const ServerIdBits int = 4

func getProposalId(highestNSeen int64, peerID int) int64 {
	newID := (highestNSeen >> ServerIdBits) + 1
	return (newID << ServerIdBits) | int64(peerID)
}

func parseProposalId(n int64) (int64, int) {
	if n == -1 {
		return -1, -1
	}
	return n >> ServerIdBits, int(n & ((1 << ServerIdBits) - 1))
}

func (px *Paxos) resetConnection(peerId int) {
	return
	/*
		peer := px.peers[peerId]
		peer.lock.Lock()
		defer peer.lock.Unlock()
		if peer.conn != nil {
			_ = peer.conn.Close()
			peer.conn = nil
		}
	*/
}

func (px *Paxos) establishConnection(peerId int, force bool) error {
	peer := px.peers[peerId]
	peer.lock.Lock()
	defer peer.lock.Unlock()

	if force == false && peer.conn != nil && peer.client != nil {
		return nil
	}
	if peer.conn == nil {
		// Establish connection
		conn, err := grpc.Dial(peer.Address, grpc.WithTransportCredentials(
			credentials.NewTLS(px.clientTLSConfig)))
		if err != nil {
			utils.Warning("Cannot dial %v", peer.Address)
			peer.conn = nil
			peer.client = nil
			utils.Warning("%v", err)
		} else {
			client := NewPaxosClient(conn)
			peer.conn = conn
			peer.client = client
		}
	}
	return nil
}

// The legacy Propose which does propose until we succeed.
// Non-leader can also do propose, and it will work like Basic Paxos.
func (px *Paxos) Propose(instanceId int64, value interface{}) {
	go func() {
		for {
			r := px.propose(instanceId, value, false, false, 10000)
			if r.Succeeded {
				return
			}
			if r.Reason == FailReasonServerClosing || r.Reason == FailReasonAlreadyFreed ||
				r.Reason == FailReasonAlreadyDecided {
				return
			}
			time.Sleep(time.Duration(rand.Intn(px.randomBackoffMax/4)) * time.Millisecond)
		}
	}()
}

// Force a server to become leader. All holes (missing instances) will be filled if
// election succeeds.
func (px *Paxos) ForceElectMe() ProposeResult {
	px.sequentialLock.Lock()
	instanceId := px.lastSequentialInstanceId
	px.sequentialLock.Unlock()

	result := px.propose(instanceId+1, nil, true, false, 3)
	return result
}

const (
	NoReason = iota
	FailReasonProposerNotLeader
	FailReasonAlreadyFreed
	FailReasonAlreadyDecided
	FailReasonServerClosing
	FailReasonReachMaxFailCount
	FailReasonTooManyHoles
	FailReasonInternalError
	FailReasonTooManyInstances
)

type phase1Result struct {
	succeeded bool
	retry     bool
	// note: Va is not decoded
	instances     *ArrayQueue
	phase1Skipped bool
	np            int64
	reason        int
}

func (px *Paxos) proposePhase1(instanceIdFrom int64, learnToInf bool, value []byte, mustBeLeader bool) phase1Result {
	majorityPeerCount := len(px.peers)/2 + 1
	peerCount := len(px.peers)

	result := phase1Result{
		succeeded:     false,
		retry:         false,
		instances:     nil,
		phase1Skipped: false,
		np:            -1,
		reason:        NoReason,
	}
	if atomic.LoadInt32(&px.TestParams.Unavailable) == 1 {
		result.retry = true
		return result
	}
	instance, ok := px.instances.GetAt(instanceIdFrom)
	if !ok {
		// it is already freed
		result.reason = FailReasonAlreadyFreed
		return result
	}

	instance.lock.Lock()
	if instance.Decided {
		instance.lock.Unlock()
		result.reason = FailReasonAlreadyDecided
		return result
	}
	instance.lock.Unlock()

	// Phase 1 Prepare
	// Initialize array queue and put our value
	acceptedInstances := NewArrayQueue(1)
	acceptedInstances.PopInstancesBefore(instanceIdFrom)
	myInstance, ok := acceptedInstances.GetAtOrCreateAt(instanceIdFrom)
	if !ok {
		result.reason = FailReasonAlreadyFreed
		return result
	}
	myInstance.HighestAcN = -1
	myInstance.HighestAcV = value
	if myInstance.InstanceId != instanceIdFrom {
		log.Fatalf("inconsistent instance id")
	}

	px.leaderInfoLock.Lock()
	// fast path? learnToInf never skips phase1
	if !learnToInf && px.leaderNp != -1 && instanceIdFrom >= px.skipPrepareFrom {
		// skip prepare
		px.SkippedPhase1Count++
		leaderN := px.leaderNp
		px.proposeLock.Unlock()
		px.leaderInfoLock.Unlock()
		utils.ProposeInfo("Server: %v Instance %v: Phase 1 skipped: n: %v", px.me, instanceIdFrom, leaderN)
		result.succeeded = true
		result.phase1Skipped = true
		result.instances = acceptedInstances
		result.np = leaderN
		return result
	}
	px.leaderInfoLock.Unlock()

	if mustBeLeader {
		result.succeeded = false
		result.retry = false
		result.reason = FailReasonProposerNotLeader
		return result
	}

	// Here we are still holding the propose lock
	px.varLock.Lock()
	highestNpSeen := px.highestNpSeen
	px.varLock.Unlock()

	n := getProposalId(highestNpSeen, px.me)
	n1, n2 := parseProposalId(n)
	utils.ProposeInfo("Server %v Instance %v: Phase 1 Prepare: n: %v=(%v, %v) inf: %v", px.me, instanceIdFrom, n, n1, n2, learnToInf)

	if learnToInf {
		utils.ProposeInfo("learning to inf")
	}

	hintNextN := int64(0)

	args := &PrepareArgs{
		InstanceIdFrom: instanceIdFrom,
		N:              n,
		Sender:         int32(px.me),
		LearnToInf:     learnToInf,
	}

	localLock := sync.Mutex{}
	maximumNoMoreAcceptedAfter := instanceIdFrom
	prepareFinished := false

	processReply := func(reply *PrepareReply) bool {
		localLock.Lock()
		// if prepare is already finished, it is a late reply,
		// and we just discard it
		if prepareFinished {
			localLock.Unlock()
			return false
		}
		if reply.OK {
			instanceFrom := reply.InstanceIdFrom
			instanceTo := instanceFrom
			if learnToInf {
				instanceTo = reply.NoMoreAcceptedAfter
				if instanceTo < reply.InstanceIdFrom {
					instanceTo = reply.InstanceIdFrom
				}
			}
			acceptedInstances.PopInstancesBefore(instanceFrom)
			acceptedInstances.reserve(instanceTo - instanceFrom + 1)
			if reply.Na != nil && len(reply.Na) > 0 {
				for id := instanceFrom; id <= instanceTo; id++ {
					instance, ok := acceptedInstances.GetAtOrCreateAt(id)
					if !ok {
						// freed
						continue
					}
					if reply.Na[id-instanceFrom] > instance.HighestAcN {
						instance.HighestAcN = reply.Na[id-instanceFrom]
						instance.HighestAcV = reply.Va[id-instanceFrom]
					}
				}
			}
			if reply.NoMoreAcceptedAfter > maximumNoMoreAcceptedAfter {
				maximumNoMoreAcceptedAfter = reply.NoMoreAcceptedAfter
			}
			if learnToInf && acceptedInstances.Size() != maximumNoMoreAcceptedAfter-acceptedInstances.GetFirstIndex()+1 {
				log.Fatalf("maximumNoMoreAcceptedAfter not consistent")
			}
		} else {
			if reply.HighestNpSeen > hintNextN {
				hintNextN = reply.HighestNpSeen
			}
		}
		localLock.Unlock()
		return reply.OK
	}

	prepareChannel := make(chan bool, peerCount)
	// Call myself
	px.waitGroup.Add(1)
	go func() {
		defer px.waitGroup.Done()
		myReply, _ := px.Prepare(nil, args)
		ok := processReply(myReply)
		if ok {
			// logger.Warning("n: %v, Receive ok from myself", n)
		}
		prepareChannel <- ok
	}()
	timeout := px.timeout
	if learnToInf {
		// if we are learning to infinity,
		// we should be tolerant of timeout.
		holesCount := px.Max() - instanceIdFrom
		timeout = px.timeout + time.Duration(holesCount/100)*px.additionalTimeoutPer100Holes
		utils.Warning("Server %v is learning to inf, with approximate %v holes, timeout is set to %v",
			px.me, holesCount, timeout)

		holeSize := 128
		// sample 10 instances
		for j := px.instances.GetHighestIndex() - 10; j <= px.instances.GetHighestIndex(); j++ {
			holeIns, holeOk := px.instances.GetAt(j)
			if holeOk {
				if len(holeIns.HighestAcV) > holeSize {
					holeSize = len(holeIns.HighestAcV)
				}
			}
		}
		totalHoleSize := holesCount * int64(holeSize)

		// use the proposing value to estimate the msg size.
		if totalHoleSize > MaxGRPCMsgSize/4 {
			utils.Error("Server %v: too many holes, with approximate size %v MB, "+
				"so server rejects to elect itself as leader.", px.me,
				totalHoleSize/1024/1024)
			result.succeeded = false
			result.reason = FailReasonTooManyHoles
			return result
		}
	}
	// call peers
	for id, pr := range px.peers {
		if id != px.me {
			// concurrently do propose
			// get a copy
			peerID := id
			peer := pr
			px.waitGroup.Add(1)
			go func() {
				defer px.waitGroup.Done()
				errConn := px.establishConnection(peerID, false)
				if errConn != nil {
					utils.ProposeNetworkError("Instance %v: Phase1 n = %v network err %v from %v", instanceIdFrom, n, errConn, peerID)
					prepareChannel <- false
					return
				}
				ctx, funcCancel := context.WithTimeout(context.Background(), timeout)
				reply, err := peer.client.Prepare(ctx, args, grpc.MaxCallRecvMsgSize(MaxGRPCMsgSize))
				funcCancel()
				ok := false
				if err == nil {
					ok = processReply(reply)
					utils.ProposePhaseInfo("n: %v, Receive reply from %v", n, peerID)
				}
				if err != nil {
					errStatus, _ := status.FromError(err)
					if errStatus.Message() != hitDropReply && errStatus.Message() != hitDropRequest {
						utils.ProposeNetworkError("Instance %v: Phase1 n = %v network err %v from %v", instanceIdFrom, n, err, peerID)
					}
					px.resetConnection(peerID)
				}

				prepareChannel <- ok
			}()
		}
	}
	allResponse := 0
	prepareOKCount := 0
	for {
		prepared := <-prepareChannel
		if prepared {
			prepareOKCount++
		}
		allResponse++
		if prepareOKCount >= majorityPeerCount {
			// wait for all peers
			// break
		}
		if allResponse >= peerCount {
			break
		}
	}

	utils.ProposeInfo("Server %v Instance %v: Phase 1 Prepare Done with OKCount %v, n: %v=(%v, %v)",
		px.me, instanceIdFrom, prepareOKCount, n, n1, n2)

	if prepareOKCount < majorityPeerCount {
		// failed
		px.varLock.Lock()
		if hintNextN > px.highestNpSeen {
			px.highestNpSeen = hintNextN
		}
		px.varLock.Unlock()
		result.retry = true
		return result
	}

	localLock.Lock()
	// We set prepareFinished to true, so from now on,
	// non-returned prepare RPC will not affect acceptedInstances
	prepareFinished = true
	localLock.Unlock()

	px.leaderInfoLock.Lock()
	if n > px.leaderNp {
		px.leaderNp = n
		if px.skipPrepareFrom > maximumNoMoreAcceptedAfter+1 {
			// This can happen, because we only learn from majority!
			// However, as long as it is not decided, we are safe.
			// log.Fatalf("maximumNoMoreAcceptedAfter GO BACKWARDS!")
		}
		px.skipPrepareFrom = maximumNoMoreAcceptedAfter + 1

		// allow nextInstanceId to go backwards, see details in ProposeValue method
		//if px.skipPrepareFrom > px.nextInstanceId {
		px.nextInstanceId = px.skipPrepareFrom
		//}
	}
	px.leaderInfoLock.Unlock()

	result.succeeded = true
	result.instances = acceptedInstances
	result.np = n
	return result
}

func (px *Paxos) proposePhase2(phase1 phase1Result, learnToInf bool, instanceIdFrom int64, maxFailCount int) (bool, bool) {
	// Phase 2 Accept:
	majorityPeerCount := len(px.peers)/2 + 1
	peerCount := len(px.peers)

	if atomic.LoadInt32(&px.TestParams.Unavailable) == 1 {
		return false, false
	}

	if phase1.instances.Size() == 0 {
		// we have nothing to propose, instances are already freed
		utils.Warning("Propose Phase2 ends because instances are already freed")
		return false, false
	}

	start := phase1.instances.GetFirstIndex()
	end := phase1.instances.GetHighestIndex()

	if learnToInf {
		utils.ProposeInfo("Server %v Instance %v: Phase 1 Prepare: n: %v inf: %v record length: %v",
			px.me, instanceIdFrom, phase1.np, learnToInf, end-start+1)
	}

	values := make([][]byte, end-start+1)
	for id := start; id <= end; id++ {
		ins, ok := phase1.instances.GetAt(id)
		if !ok {
			log.Fatalf("should be ok")
		}
		values[id-start] = ins.HighestAcV
	}

	holesCount := end - start + 1

	accArgs := &AcceptArgs{
		InstanceIdFrom: start,
		InstanceIdTo:   end,
		N:              phase1.np,
		V:              values,
		Sender:         int32(px.me),
	}

	acceptFailCount := -1
	randomBackoff := 20
	for {
		acceptChannel := make(chan int, peerCount)
		const acceptOK = 1
		const acceptNetworkErr = 2
		const acceptRejected = 3

		acceptFailCount++
		if acceptFailCount > maxFailCount {
			return false, false
		}

		if acceptFailCount > 0 && !phase1.phase1Skipped {
			// Do not retry if we did not skip phase 1
			// since we are holding proposingLock
			return false, false
		}

		if atomic.LoadInt32(&px.closed) == 1 {
			return false, false
		}

		if acceptFailCount > 0 {
			randomBackoff = randomBackoff * 2
			if randomBackoff > px.randomBackoffMax {
				randomBackoff = px.randomBackoffMax
			}
			randomSleepTime := rand.Int() % randomBackoff
			utils.ProposeInfo("Server %v: When proposing %v (phase2), Forced to sleep %vms (backoff:%v)", px.me, instanceIdFrom, randomSleepTime, randomBackoff)
			time.Sleep(time.Duration(randomSleepTime) * time.Millisecond)
		}

		// call myself
		px.waitGroup.Add(1)
		go func() {
			defer px.waitGroup.Done()
			accReply, err := px.Accept(nil, accArgs)
			if err == nil && accReply.OK {
				phase1.instances.PopInstancesBefore(accReply.InstanceIdFrom)
				acceptChannel <- acceptOK
				//logger.Warning("Instance %v: Phase2 : n : %v, accept from myself", instanceIdFrom, phase1.np)
			} else {
				acceptChannel <- acceptRejected
			}
		}()
		// call peers
		timeout := px.timeout
		if learnToInf {
			timeout = px.timeout + time.Duration(holesCount/100)*px.additionalTimeoutPer100Holes
			utils.Warning("Server %v is learning to inf, Accept phase with %v holes, timeout is set to %v",
				px.me, holesCount, timeout)
		}
		for id, pr := range px.peers {
			if id != px.me {
				peerID := id
				peer := pr
				px.waitGroup.Add(1)
				go func() {
					defer px.waitGroup.Done()
					errConn := px.establishConnection(peerID, false)
					if errConn != nil {
						acceptChannel <- acceptNetworkErr
						utils.ProposeNetworkError("Instance %v: Phase2 n = %v, accept network err %v from %v", instanceIdFrom, phase1.np, errConn, peerID)
						return
					} else {
						ctx, funcCancel := context.WithTimeout(context.Background(), timeout)
						reply, err := peer.client.Accept(ctx, accArgs, grpc.MaxCallSendMsgSize(MaxGRPCMsgSize))
						funcCancel()
						if err != nil {
							px.resetConnection(peerID)
							acceptChannel <- acceptNetworkErr
							errStatus, _ := status.FromError(err)
							if errStatus.Message() != hitDropReply && errStatus.Message() != hitDropRequest {
								utils.ProposeNetworkError("Instance %v: Phase2 n = %v, accept network err %v from %v", instanceIdFrom, phase1.np, err, peerID)
							}
							return
						} else {
							if reply.OK {
								phase1.instances.PopInstancesBefore(reply.InstanceIdFrom)
								acceptChannel <- acceptOK
								utils.ProposePhaseInfo("Instance %v: Phase2 n = %v, accept from %v", instanceIdFrom, phase1.np, peerID)
								return
							} else {
								utils.ProposePhaseInfo("Instance %v: Phase2 n = %v, accept rejected by %v", instanceIdFrom, phase1.np, peerID)
								acceptChannel <- acceptRejected
								return
							}
						}
					}
				}()
			}
		}
		acceptOKCount := 0
		allAcceptResponse := 0
		acceptRejectCount := 0
		for {
			acceptResult := <-acceptChannel
			if acceptResult == acceptOK {
				acceptOKCount++
			} else if acceptResult == acceptRejected {
				acceptRejectCount++
			}
			allAcceptResponse++
			if acceptOKCount >= majorityPeerCount {
				// wait for all peers
				// break
			}
			if allAcceptResponse >= peerCount {
				break
			}
		}

		if acceptOKCount < majorityPeerCount {
			// failed
			if phase1.phase1Skipped {
				// WE ARE NOT HOLDING proposalLock HERE
				// fails only if we are rejected
				// retry from phase 2 otherwise
				if acceptRejectCount > 0 {
					// DO NOT RETRY
					// see details about nextInstanceId in ProposeValue method
					return false, false
				} else {
					// retry from phase 2
					// see details about nextInstanceId in ProposeValue method
					continue
				}
			} else {
				// we did not skip phase 1,
				// we should retry from phase 2
				// continue
				// OR do not retry
				return false, false
			}
		} else {
			// succeeded
			return true, false
		}
	}
}

type ProposeResult struct {
	Succeeded  bool
	Reason     int
	InstanceId int64
}

const MaxGRPCMsgSize = 512 * 1024 * 1024
const ChooseInstanceIdAuto = -1000

func (px *Paxos) propose(instanceIdFrom int64, value interface{}, learnToInf bool, mustBeLeader bool, maxFailCount int) ProposeResult {

	// ----------------------------------------------------
	//  Entry
	// ----------------------------------------------------

	// A proposal in Prepare phase either succeeds or fails.
	// No automatic retry is supported.
	// However, if we do not hold proposalLock, propose will
	// automatically retry Accept phase until maxFailCount is reached.

	if atomic.LoadInt32(&px.closed) == 1 {
		return ProposeResult{
			Succeeded: false,
			Reason:    FailReasonServerClosing,
		}
	}

	// ----------------------------------------------------
	//  Propose begins
	// ----------------------------------------------------
	if instanceIdFrom != ChooseInstanceIdAuto {
		// If less than Min(), it should be ignored:
		if instanceIdFrom < px.getMin() {
			return ProposeResult{Succeeded: false, Reason: FailReasonAlreadyFreed}
		}
	}

	encodedValue := px.encodeValue(value)

	// try to acquire proposeLock here
	// because nextInstanceId might go backwards
	// make sure the last learnToInf proposal ends.
	if instanceIdFrom > 0 {
		// fmt.Printf("Lock: Server %v, ins %v is waiting proposeLock, if blocks, %v is holding the lock\n", px.me, instanceIdFrom, px.whichInstanceHoldProposalLock)
	}
	px.proposeLock.Lock()
	if instanceIdFrom > 0 {
		// fmt.Printf("Lock: Server %v, ins %v gets proposeLock\n", px.me, instanceIdFrom)
	}
	px.leaderInfoLock.Lock()
	if instanceIdFrom == ChooseInstanceIdAuto {
		instanceIdFrom = px.nextInstanceId
		px.nextInstanceId++
		if mustBeLeader == false {
			log.Fatalf("ChooseInstanceIdAuto not valid.")
		}
	}
	px.leaderInfoLock.Unlock()

	px.whichInstanceHoldProposalLock = instanceIdFrom

	instance, ok := px.instances.GetAtOrCreateAt(instanceIdFrom)
	if !ok {
		px.proposeLock.Unlock()
		return ProposeResult{Succeeded: false, Reason: FailReasonAlreadyFreed}
	}

	instance.lock.Lock()
	if instance.Decided == true {
		instance.lock.Unlock()
		px.proposeLock.Unlock()
		return ProposeResult{Succeeded: false, Reason: FailReasonAlreadyDecided}
	}
	instance.lock.Unlock()

	phase1 := px.proposePhase1(instanceIdFrom, learnToInf, encodedValue, mustBeLeader)
	if !phase1.succeeded {
		px.proposeLock.Unlock()
		// fmt.Printf("Lock Release: Server %v, ins %v releases proposeLock\n", px.me, instanceIdFrom)

		return ProposeResult{
			Succeeded: false,
			Reason:    phase1.reason,
		}
	}

	phase2Succeeded, _ := px.proposePhase2(phase1, learnToInf, instanceIdFrom, maxFailCount)

	if !phase2Succeeded {
		// invalidate leaderNp here, because
		// phase 2 does not reset leaderNp if it fails or reaches
		// its own max fail count
		// we should also invalidate leaderNp if !phase1Skipped
		// because the election might fail with leaderNp set.
		px.leaderInfoLock.Lock()
		px.leaderNp = -1
		px.leaderInfoLock.Unlock()

		if !phase1.phase1Skipped {
			px.proposeLock.Unlock()
		}
		// fmt.Printf("Lock Release: Server %v, ins %v releases proposeLock\n", px.me, instanceIdFrom)

		return ProposeResult{
			Succeeded: false,
			Reason:    FailReasonReachMaxFailCount,
		}
	}

	// proposal succeeded
	if !phase1.phase1Skipped {
		px.proposeLock.Unlock()
	}
	// fmt.Printf("Lock Release: Server %v, ins %v releases proposeLock\n", px.me, instanceIdFrom)

	if phase1.phase1Skipped && px.FastPathProposalIdCollisionDetectorEnabled {
		go func() {
			foundValue, alreadyExist := px.fastPathProposalIdCollisionRecord.LoadOrStore(instanceIdFrom, value)
			if alreadyExist {
				if !reflect.DeepEqual(value, foundValue) {
					log.Fatalf("proposal id collision %v proposing value %v, but already decided %v", instanceIdFrom, value, foundValue)
				}
			}
		}()
	}

	// Phase 3 Decide:
	utils.ProposeInfo("Server %v Instance %v: Phase 3 Decide: n: %v",
		px.me, instanceIdFrom, phase1.np)

	if atomic.LoadInt32(&px.TestParams.Unavailable) == 1 {
		return ProposeResult{
			Succeeded: false,
			Reason:    NoReason,
		}
	}

	if phase1.instances.Size() == 0 {
		// we have nothing to propose, instances are already freed
		utils.Warning("discarding instances!")
		return ProposeResult{
			Succeeded: false,
			Reason:    FailReasonAlreadyFreed,
		}
	}

	start := phase1.instances.GetFirstIndex()
	end := phase1.instances.GetHighestIndex()
	values := make([][]byte, end-start+1)
	for id := start; id <= end; id++ {
		ins, ok := phase1.instances.GetAt(id)
		if !ok {
			log.Fatalf("should be ok")
		}
		values[id-start] = ins.HighestAcV
	}

	holesCount := end - start + 1

	args := &DecideArgs{
		InstanceIdFrom:    start,
		InstanceIdTo:      end,
		V:                 values,
		Sender:            int32(px.me),
		MinimumInstanceId: px.getMinimumInstanceIdNeeded(px.me),
	}

	// make sure we are notified of decision before return
	_, _ = px.Decide(nil, args)

	timeout := px.timeout
	if learnToInf {
		timeout = px.timeout + time.Duration(holesCount/100)*px.additionalTimeoutPer100Holes
		utils.Warning("Server %v is learning to inf, Decide phase with %v holes, timeout is set to %v",
			px.me, holesCount, timeout)
	}
	for id, pr := range px.peers {
		peerID := id
		peer := pr
		px.waitGroup.Add(1)
		go func() {
			defer px.waitGroup.Done()
			if peerID != px.me {
				// keep trying to avoid missing logs, since we do not yet
				// have a strategy to ask for logs if applier cannot make progress
				for tryNotify := int64(0); tryNotify < 10; tryNotify++ {
					if atomic.LoadInt32(&px.closed) == 1 {
						break
					}
					errConn := px.establishConnection(peerID, false)
					if errConn != nil {
						continue
					}
					ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
					decideReply, err := peer.client.Decide(ctx, args, grpc.MaxCallSendMsgSize(MaxGRPCMsgSize))
					cancelFunc()
					if err != nil {
						px.resetConnection(peerID)
					} else if decideReply.OK {
						px.updateMinimumInstanceIdNeeded(peerID, decideReply.MinimumInstanceId)
						break
					}
					if atomic.LoadInt32(&px.closed) == 1 {
						break
					}
					sleepTime := tryNotify*1000 + 1000
					time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				}
			}
		}()
	}

	// OK.
	return ProposeResult{
		Succeeded:  true,
		Reason:     NoReason,
		InstanceId: instanceIdFrom,
	}
}

func (px *Paxos) GetLeaderInfo() (int64, int64) {
	px.leaderInfoLock.Lock()
	defer px.leaderInfoLock.Unlock()
	// protected by proposeLock
	return px.leaderNp, px.skipPrepareFrom
}

type ProposeValueResult struct {
	Succeeded  bool
	Reason     int
	LeaderHint int
	InstanceId int64
}

//  Just propose a value, ProposeValue will check leadership, select an instance id and do propose.
//  If we are not leader, ProposeValue will fail.
//  ProposeValue will block until our value is decided OR we fail.
//  ProposeValue can be concurrently called.
func (px *Paxos) ProposeValue(value interface{}, isSameValue func(interface{}, interface{}) bool) *ProposeValueResult {
	retVal := &ProposeValueResult{
		Succeeded:  false,
		Reason:     NoReason,
		LeaderHint: -1,
		InstanceId: -1,
	}

	throttleMaxSequential := px.MaxSequential()
	throttleMaxInstance := px.Max()

	if throttleMaxInstance-throttleMaxSequential > int64(grpcConcurrentStreams) {
		retVal.Reason = FailReasonTooManyInstances
		utils.Warning("Paxos throttle! Max: %v, MaxSeq: %v", throttleMaxInstance, throttleMaxSequential)
		return retVal
	}

	px.varLock.Lock()
	_, peer := parseProposalId(px.highestNpSeen)
	px.varLock.Unlock()
	retVal.LeaderHint = peer

	leaderNp, _ := px.GetLeaderInfo()
	if leaderNp == -1 {
		retVal.Reason = FailReasonProposerNotLeader
		return retVal
	}

	// we can safely use nextInstanceId and avoid holes as long as we are leader,
	// because we fill in all the holes before this value when becoming the leader
	// WHY we should NOT retry from phase 1 once we enter phase 1:
	// consider this scenario: we have noMoreAcceptedAfter = 100,
	//    we've proposed with 101,102,...,110. None of them succeeds, and
	//    all of them are retrying. 101-104 do not skip phase 1. 105-110 skip phase 1.
	//    We lost leadership, but wins election with a newer leaderNp again.
	//    Since 101,102,...,110 by default retry from phase 1,
	//    these instances will use the new leaderNp to propose value 1.
	//    Even 101-104 which originally do not skip phase 1 will skip phase 1 now!
	//    However, at the same time, since no server accepts 101,..,110,
	//    the nextInstanceId becomes 101 again.
	//    A new ProposeValue method call with value2 starts again from 101,
	//    and causes value collision.
	//
	// To deal with this problem, I choose to allow nextInstanceId to go backwards in order to avoid holes,
	// but once a proposal enters phase 2 (no matter whether it skips phase 1),
	// it cannot restart from phase 1.
	// It must retry from phase 2 with the original(old) np
	// until it gets rejected or exceeds max try times.
	// Note: leaderNp, skipPrepareFrom, nextInstanceId should be updated under proposeLock

	result := px.propose(ChooseInstanceIdAuto, value, false, true, 10)
	if result.Succeeded {
		ok, decidedV := px.GetInstance(result.InstanceId)
		if ok && isSameValue(value, decidedV) {
			retVal.Succeeded = true
			retVal.InstanceId = result.InstanceId
			return retVal
		} else {
			retVal.Reason = FailReasonInternalError
			return retVal
		}
	} else {
		retVal.Reason = result.Reason
		return retVal
	}
}

func (px *Paxos) GetPeerAddress(peerId int) string {
	return px.peers[peerId].Address
}

func (px *Paxos) getMinimumInstanceIdNeeded(peerId int) int64 {
	px.doneLock.Lock()
	defer px.doneLock.Unlock()
	return px.peers[peerId].MinimumInstanceId
}

func (px *Paxos) updateMinimumInstanceIdNeeded(peerId int, done int64) {
	px.doneLock.Lock()
	if done > px.peers[peerId].MinimumInstanceId {
		px.peers[peerId].MinimumInstanceId = done
	}
	px.doneLock.Unlock()
}

func (px *Paxos) GetMe() int {
	return px.me
}

// the application no longer needs instances <= instanceId.
func (px *Paxos) Done(instanceId int64) {
	go px.updateMinimumInstanceIdNeeded(px.me, instanceId+1)
}

// the application wants to know the
// highest instance ID known to
// this peer.
//
func (px *Paxos) Max() int64 {
	return px.instances.GetHighestIndex()
}

func (px *Paxos) Min() int64 {
	return px.getMin()
}

func (px *Paxos) MaxSequential() int64 {
	px.sequentialLock.Lock()
	defer px.sequentialLock.Unlock()
	return px.lastSequentialInstanceId
}

func (px *Paxos) getMin() int64 {
	var doneByAll int64 = math.MaxInt64
	px.doneLock.Lock()
	for _, peer := range px.peers {
		if peer.MinimumInstanceId < doneByAll {
			doneByAll = peer.MinimumInstanceId
		}
	}
	px.doneLock.Unlock()
	return doneByAll
}

// Still working on this.
// dump instances < instanceId and free memory
func (px *Paxos) dumpInstance(instanceId int64) {
	px.instances.PopInstancesBefore(instanceId)
}

// TODO:
/*
func (px *Paxos) readInstanceFromDisk(instanceId int64) (*Instance, bool) {
	instance := &Instance{InstanceId: instanceId}
	storageIns, storageOk := px.instancesToStorageKey.GetAtOrCreateAt(instanceId)
	if !storageOk {
		// this instance is deleted from disk storage
		// after all servers reach consensus on Done
		return nil, false
	}
}

*/

// Free instances < getMin().
func (px *Paxos) cleanDoneInstances() {
	px.doneInstancesCleanerLock.Lock()
	defer px.doneInstancesCleanerLock.Unlock()

	currentMin := px.getMin()

	if px.instances.GetFirstIndex() >= currentMin {
		// nothing to free
		return
	}
	utils.Warning("Server %v Done instances' collector is freeing instances < %v",
		px.me, currentMin)
	px.instances.PopInstancesBefore(currentMin)
	// Look up storage and delete disk files
	storageIns := px.instancesToStorageKey.PopInstancesBefore(currentMin)
	if storageIns == nil {
		return
	}
	for _, ins := range storageIns {
		ins.lock.Lock()
		for _, key := range ins.tag {
			err := px.storage.Delete(key)
			if err != nil {
				log.Fatalf("cleanDoneInstances: cannot delete %v, err: %v", key, err)
			}
		}
		ins.lock.Unlock()
	}
}

func (px *Paxos) GetInstance(instanceId int64) (bool, interface{}) {
	instance, found := px.instances.GetAt(instanceId)
	if !found {
		return false, nil
	}
	instance.lock.Lock()
	defer instance.lock.Unlock()
	if instance.Decided {
		return true, instance.DecidedV
	}
	return false, nil
}

// Gracefully shutdown Paxos
func (px *Paxos) Close() {
	atomic.StoreInt32(&px.closed, 1)
	if px.server != nil {
		px.server.GracefulStop()
		px.server = nil
	}

	// wait for all goroutines to finish
	px.waitGroup.Wait()
}

//  ---- Start the heartbeat goroutine
func (px *Paxos) startHeartBeat() {
	// Initialize all peers' response to now
	for _, peer := range px.peers {
		peer.LastResponseTime = time.Now()
		peer.ApproximateRTT = 0
	}
	px.waitGroup.Add(1)
	go func() {
		defer px.waitGroup.Done()
		for atomic.LoadInt32(&px.closed) == 0 {
			if atomic.LoadInt32(&px.Unavailable) == 1 {
				time.Sleep(time.Duration(px.heartbeatInterval) * time.Millisecond)
				continue
			}
			px.waitGroup.Add(1)
			go func() {
				defer px.waitGroup.Done()
				px.heartbeat()
			}()
			time.Sleep(time.Duration(px.heartbeatInterval) * time.Millisecond)
		}
	}()
}

func (px *Paxos) heartbeat() {

	heartbeatReplies := make([]*HeartbeatReply, len(px.peers))

	localLock := &sync.Mutex{}
	args := &HeartbeatArgs{
		Sender:                int32(px.me),
		MinimumInstanceId:     px.getMinimumInstanceIdNeeded(px.me),
		HighestInstanceIdSeen: px.Max(),
	}
	beatChan := make(chan bool, len(px.peers))
	for id, pr := range px.peers {
		if id != px.me {
			peerID := id
			peer := pr
			go func() {
				err := px.establishConnection(peerID, false)
				if err != nil {
					beatChan <- false
					return
				}
				tSent := time.Now()
				ctx, funcCancel := context.WithTimeout(context.Background(), px.timeout)
				reply, err := peer.client.Heartbeat(ctx, args)
				funcCancel()
				if err != nil {
					px.resetConnection(peerID)
					utils.HeartBeatError("heartbeat failed: %v", err)
					beatChan <- false
					return
				}
				if reply.OK {
					px.updateMinimumInstanceIdNeeded(peerID, reply.MinimumInstanceId)
					px.instances.GetAtOrCreateAt(reply.HighestInstanceIdSeen)
					peer.lock.Lock()
					peer.LastResponseTime = time.Now()
					if peer.ApproximateRTT == 0 {
						peer.ApproximateRTT = time.Since(tSent)
					} else {
						peer.ApproximateRTT = peer.ApproximateRTT*7/8 +
							time.Since(tSent)/8
					}
					peer.lock.Unlock()
					localLock.Lock()
					heartbeatReplies[peerID] = reply
					localLock.Unlock()
				}
				beatChan <- reply.OK
			}()
		}
	}
	// here, we wait for all peers until timeout
	// 1 from myself
	cnt := 1
	okCnt := 1
	for {
		ok := <-beatChan
		cnt++
		if ok {
			okCnt++
		}
		if cnt == len(px.peers) {
			break
		}
	}

	px.cleanDoneInstances()

	// If we are able to communicate with the majority, and
	if okCnt >= len(px.peers)/2+1 {
		// if we haven't heard from the leader for a long time or there is no leader
		// we automatically do takeover
		// the peer with smallest id will do a no-op propose.
		// it is ok if servers do not reach consensus about this smallest id,
		// as finally only one server wins.

		// generate heartbeat info:
		// localLock.Lock()
		utils.Debug("%v reports that it is with majority, and heartbeat reply is %v",
			px.me, heartbeatReplies)
		// localLock.Unlock()
		px.checkLeaderAliveness(heartbeatReplies)
	}
}

func (px *Paxos) checkLeaderAliveness(replies []*HeartbeatReply) {
	leaderNp, _ := px.GetLeaderInfo()
	px.varLock.Lock()
	highestNp := px.highestNpSeen
	px.varLock.Unlock()

	for _, reply := range replies {
		if reply != nil {
			if reply.LeaderNp > leaderNp {
				leaderNp = reply.LeaderNp
			}
			if reply.HighestNp > highestNp {
				highestNp = reply.HighestNp
			}
		}
	}
	px.varLock.Lock()
	if highestNp > px.highestNpSeen {
		// useful if we need to elect ourselves later
		px.highestNpSeen = highestNp
	}
	px.varLock.Unlock()

	implicitLeader := -1
	if highestNp != -1 {
		_, implicitLeader = parseProposalId(highestNp)
	}
	if implicitLeader == px.me {
		// we are the leader
		// check whether we think that we are leader?
		myNp, _ := px.GetLeaderInfo()
		if myNp == -1 {
			// dangerous, all peers think we are leader, but we are rejecting all requests
			utils.Warning("Server %v does not know it is leader, and is re-electing itself as leader", px.me)
			px.electMe()
		}
		// nothing to do
		return
	}
	leaderAlive := true
	if implicitLeader < 0 {
		leaderAlive = false
	} else if replies[implicitLeader] == nil {
		// the leader does not reply
		px.peers[implicitLeader].lock.Lock()
		lastResponseTime := px.peers[implicitLeader].LastResponseTime
		rtt := px.peers[implicitLeader].ApproximateRTT
		px.peers[implicitLeader].lock.Unlock()
		utils.Info("Server %v does not get reply from implicit leader %v, "+
			"its LastResponseTime is %v", px.me, implicitLeader, lastResponseTime)

		timeout := px.leaderTimeout
		if rtt < 10*time.Millisecond {
			rtt = 10 * time.Millisecond
		}
		timeout2 := px.heartbeatInterval + int(rtt.Milliseconds())*px.leaderTimeoutRTTs
		if timeout2 < timeout {
			timeout = timeout2
		}
		if time.Since(lastResponseTime) > time.Duration(timeout)*time.Millisecond {
			leaderAlive = false
		}

	} else {
		// the implicit leader replies
		if replies[implicitLeader].LeaderNp == -1 {
			// the implicit leader does not consider itself as leader
			// mark leader as dead?
			// leaderAlive = false
			// give implicit leader a chance to re-elect itself
		}
	}

	if !leaderAlive {
		alive := px.me
		// find out who has the minimum peer id
		for peerId, reply := range replies {
			if reply != nil && peerId < alive {
				alive = peerId
			}
		}
		utils.Warning("Server %v thinks leader %v is lost, and alive minimum peer id is %v", px.me, implicitLeader, alive)
		if alive == px.me {
			utils.Warning("Server %v thinks leader %v is lost, and is electing itself as leader", px.me, implicitLeader)
			px.electMe()
		}
	}
}

func (px *Paxos) electMe() {
	px.sequentialLock.Lock()
	instanceId := px.lastSequentialInstanceId
	px.sequentialLock.Unlock()
	// propose to get and propagate undecided instances
	px.waitGroup.Add(1)
	go func() {
		defer px.waitGroup.Done()
		px.electLock.Lock()
		if px.isElecting {
			// give up this chance, because we are still electing
			px.electLock.Unlock()
			utils.Warning("Server %v gives up the electing chance", px.me)
			return
		}
		// are we leader?
		//myNp, _ := px.GetLeaderInfo()
		//if myNp != -1 {
		///	px.electLock.Unlock()
		//	return
		//}
		px.isElecting = true
		px.electLock.Unlock()
		px.propose(instanceId+1, nil, true, false, 3)
		px.electLock.Lock()
		px.isElecting = false
		px.electLock.Unlock()
	}()
}

func (px *Paxos) StartServer() {
	RegisterPaxosServer(px.server, px)

	// tests will create listener themselves
	if px.l == nil {
		l, err := net.Listen("tcp", px.peers[px.me].Address)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
		px.l = l
	}

	go func() {
		err := px.server.Serve(px.l)
		if err != nil {
			utils.Error("serve err: %v", err)
		}
	}()

	px.startHeartBeat()
}

type ServerConfig struct {
	Peers             []*Peer
	Me                int
	Storage           PersistentStorage
	Timeout           time.Duration
	RandomBackoffMax  int
	HeartbeatInterval int
	LeaderTimeoutRTTs int
	LeaderTimeout     int
	DecideChannel     chan DecideEvent
	// tests might need this
	Listener                     net.Listener
	AdditionalTimeoutPer100Holes time.Duration
}

func NewPaxos(config *ServerConfig, tlsConfig *utils.MutualTLSConfig) (*Paxos, error) {
	gob.Register(&ValueWrapper{})
	rand.Seed(time.Now().UTC().UnixNano())

	px := &Paxos{
		me:                           config.Me,
		peers:                        make([]*Peer, 0),
		instances:                    NewArrayQueue(1),
		l:                            config.Listener,
		leaderNp:                     -1,
		highestNpSeen:                -1,
		highestInstanceAccepted:      -1,
		lastSequentialInstanceId:     -1,
		timeout:                      config.Timeout,
		randomBackoffMax:             config.RandomBackoffMax,
		heartbeatInterval:            config.HeartbeatInterval,
		leaderTimeoutRTTs:            config.LeaderTimeoutRTTs,
		leaderTimeout:                config.LeaderTimeout,
		additionalTimeoutPer100Holes: config.AdditionalTimeoutPer100Holes,
		decideSequentialChan:         config.DecideChannel,
		TestParams: TestParams{
			NotRespondingRPC:       0,
			Unreliable:             0,
			DropRequestProbability: 0,
			DropReplyProbability:   0,
			RpcCount:               0,
			LatencyLowerBound:      0,
			LatencyUpperBound:      0,
		},
		instancesToStorageKey: NewArrayQueue(1),
	}

	// copy peers' data to avoid shared pointer
	for _, peer := range config.Peers {
		clonedPeer := &Peer{
			ServerName:        peer.ServerName,
			Address:           peer.Address,
			MinimumInstanceId: 0,
			conn:              nil,
			client:            nil,
			lock:              sync.Mutex{},
			LastResponseTime:  time.Now(),
			ApproximateRTT:    0,
		}
		px.peers = append(px.peers, clonedPeer)
	}

	px.storage = config.Storage

	serverTLSConfig, clientTLSConfig, err := utils.LoadMutualTLSConfig(tlsConfig)

	if err != nil {
		return nil, err
	}

	px.clientTLSConfig = clientTLSConfig

	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig)),
		grpc.MaxRecvMsgSize(MaxGRPCMsgSize), grpc.MaxSendMsgSize(MaxGRPCMsgSize),
		grpc.MaxConcurrentStreams(uint32(grpcConcurrentStreams)))

	px.server = server

	return px, nil
}

// ------------------------------------------------
//   Paxos Instances Reconstruction/Recover

//  Reconstruct Paxos: load instances from disk files and
//       create a Paxos consistent to the one before
//       crashing.
//
type ReconstructionResult struct {
	MinimumInstanceId int64 // the smallest instance we can recover
	LogEntriesCount   int64 // how many log entries we've found
}

//  Reconstruct Paxos if server crashed
//  Should be called before StartServer
//  Warning: caller must pass a new storage(a new folder) to NewPaxos, different from where logFiles lie in, and
//  ReconstructPaxos cannot check this.
//  Recovered logs will be written to the ServerConfig.Storage by calling the Write method.
func (px *Paxos) ReconstructPaxos(logFiles []string) (*ReconstructionResult, error) {

	id2ins := map[int64]*Instance{}
	highestInstanceId := int64(-1)
	minimumInstanceId := int64(-1)
	logEntriesCount := int64(0)

	for _, file := range logFiles {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		emptyBuf := make([]byte, 1024)
		for {
			entryContent := bytes.Buffer{}
			buf := make([]byte, 1024)
			_, err = io.ReadFull(f, buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if buf[0] == 0 {
				if bytes.Equal(emptyBuf, buf) {
					continue
				}
			}
			entryContent.Write(buf)
			if !bytes.Equal(buf[0:len(logPrefix)], []byte(logPrefix)) {
				return nil, errors.New("paxos log corrupted")
			}
			index := len(logPrefix)
			length := int(binary.BigEndian.Uint32(buf[index : index+4]))
			entryLength := len(logPrefix) + len(logSuffix) + 8 + length
			padding := 0
			if entryLength%1024 != 0 {
				padding = 1024 - entryLength%1024
			}
			if entryLength+padding > 1024 {
				buf2 := make([]byte, entryLength+padding)
				_, err = io.ReadFull(f, buf2)
				if err == io.EOF {
					return nil, errors.New("reconstruction: unexpected EOF")
				}
				if err != nil {
					return nil, err
				}
				entryContent.Write(buf2)
			}
			instance, np, high, err := px.decodeInstance(entryContent.Bytes())
			if err != nil {
				return nil, err
			}
			id := instance.InstanceId
			if id == -1 {
				// this is a prepare log, no instance info
			} else {
				if id2ins[id] == nil || instance.HighestAcN > id2ins[id].HighestAcN {
					id2ins[id] = instance
				}
				if high > highestInstanceId {
					highestInstanceId = high
				}
				if id < minimumInstanceId || minimumInstanceId == -1 {
					minimumInstanceId = id
				}
			}

			//fmt.Printf("np: %v, instance: %v\n", np, instance)
			if np > px.highestNpSeen {
				px.highestNpSeen = np
			}

			logEntriesCount++
		}
	}

	px.instances.GetAtOrCreateAt(highestInstanceId)
	for _, ins := range id2ins {
		inst, found := px.instances.GetAt(ins.InstanceId)
		if !found {
			log.Fatalf("maybe queue impl error? or log corrupted")
		}
		if inst.InstanceId != ins.InstanceId {
			log.Fatalf("queue impl error, inconsistent instance id")
		}

		inst.HighestAcN = ins.HighestAcN
		inst.HighestAcV = ins.HighestAcV
	}

	if minimumInstanceId == -1 {
		return nil, errors.New("reconstruction: no instance found")
	}

	for i := minimumInstanceId; i <= highestInstanceId; i++ {
		// write to new folder
		instance, ok := px.instances.GetAt(i)
		if !ok {
			return nil, errors.New("maybe queue wrong impl")
		}
		if instance.InstanceId != i {
			return nil, errors.New("maybe queue wrong impl")
		}
		if instance != nil {
			key := strconv.Itoa(int(instance.InstanceId)) + "-" + strconv.Itoa(int(px.highestNpSeen))
			length, content := px.encodeInstance(instance, px.highestNpSeen, highestInstanceId)

			err := px.storage.Write(key, length, content)
			if err != nil {
				log.Fatalf("cannot write paxos log " + err.Error())
			}
			if instance.HighestAcN != -1 && instance.InstanceId > px.highestInstanceAccepted {
				px.highestInstanceAccepted = instance.InstanceId
			}
		}
	}

	result := &ReconstructionResult{
		MinimumInstanceId: minimumInstanceId,
		LogEntriesCount:   logEntriesCount,
	}
	return result, nil
}
