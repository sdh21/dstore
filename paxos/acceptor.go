package paxos

import (
	"bytes"
	"context"
	"encoding/gob"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type ValueWrapper struct {
	Value interface{}
}

func (px *Paxos) encodeValue(v interface{}) []byte {
	wrapper := &ValueWrapper{Value: v}
	if v == nil {
		return make([]byte, 0)
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(wrapper)
	if err != nil {
		utils.Error("encodeValue: %v", err)
	}
	return buf.Bytes()
}

func (px *Paxos) decodeValue(b []byte) interface{} {
	if len(b) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	v := &ValueWrapper{}
	err := dec.Decode(v)
	if err != nil {
		utils.Error("decodeValue: %v", err)
	}
	return v.Value
}

func (px *Paxos) atomicLoadFloat64(f *float64) float64 {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(f))))
}

func (px *Paxos) beforeHandleRPC(sender int32) bool {
	if !px.TestEnabled {
		return false
	}
	if atomic.LoadInt32(&px.Unavailable) == 1 {
		return true
	}
	if atomic.LoadInt32(&px.BlockList[sender]) == 1 {
		return true
	}
	latencyUb := int(atomic.LoadInt32(&px.LatencyUpperBound))
	latencyLb := int(atomic.LoadInt32(&px.LatencyLowerBound))
	if latencyUb > latencyLb {
		sleepMs := rand.Intn(latencyUb-latencyLb) + latencyLb
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}

	if atomic.LoadInt32(&px.NotRespondingRPC) == 1 {
		return true
	}
	if atomic.LoadInt32(&px.Unreliable) == 1 &&
		rand.Float64() < px.atomicLoadFloat64(&px.DropRequestProbability) {
		return true
	}

	return false
}

func (px *Paxos) afterHandleRPC() bool {
	if !px.TestEnabled {
		return false
	}
	latencyUb := int(atomic.LoadInt32(&px.LatencyUpperBound))
	latencyLb := int(atomic.LoadInt32(&px.LatencyLowerBound))
	if latencyUb > latencyLb {
		sleepMs := rand.Intn(latencyUb-latencyLb) + latencyLb
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}

	if atomic.LoadInt32(&px.NotRespondingRPC) == 1 {
		return true
	}
	if atomic.LoadInt32(&px.Unreliable) == 1 &&
		rand.Float64() < px.atomicLoadFloat64(&px.DropReplyProbability) {
		return true
	}
	return false
}

const hitDropRequest = "Test: Hit Drop Request"
const hitDropReply = "Test: Hit Drop Reply"

func (px *Paxos) Prepare(ctx context.Context, args *PrepareArgs) (*PrepareReply, error) {
	if ctx != nil && px.beforeHandleRPC(args.Sender) {
		//logger.Warning(hitDropRequest)
		return nil, status.Errorf(codes.Unavailable, hitDropRequest)
	}
	atomic.AddInt64(&px.RpcCount, 1)

	px.peers[args.Sender].lock.Lock()
	px.peers[args.Sender].LastResponseTime = time.Now()
	px.peers[args.Sender].lock.Unlock()

	reply := &PrepareReply{}

	px.varLock.Lock()
	if args.N > px.highestNpSeen {
		px.highestNpSeen = args.N
		reply.OK = true
		reply.NoMoreAcceptedAfter = px.highestInstanceAccepted
		reply.HighestNpSeen = args.N
	} else {
		reply.OK = false
		reply.HighestNpSeen = px.highestNpSeen
	}
	globalNpToStore := px.highestNpSeen

	if reply.OK {
		// default: we only need to learn one instance (the proposing one)
		learnTo := args.InstanceIdFrom
		reply.InstanceIdFrom = args.InstanceIdFrom
		if args.LearnToInf {
			// however, an elect proposal might need to learn to infinity
			learnTo = reply.NoMoreAcceptedAfter
			if learnTo < reply.InstanceIdFrom {
				learnTo = reply.InstanceIdFrom
			}
		}
		//fmt.Printf("%v %v\n", reply.InstanceIdFrom, learnTo)
		reply.Na = make([]int64, 0, learnTo-reply.InstanceIdFrom+1)
		reply.Va = make([][]byte, 0, learnTo-reply.InstanceIdFrom+1)
		for id := args.InstanceIdFrom; id <= learnTo; id++ {
			instance, ok := px.instances.GetAtOrCreateAt(id)

			if !ok {
				// instance already freed
				reply.InstanceIdFrom = id + 1
				// reset Na Va
				reply.Na = make([]int64, 0, learnTo-reply.InstanceIdFrom+1)
				reply.Va = make([][]byte, 0, learnTo-reply.InstanceIdFrom+1)
				continue
			}
			if id != instance.InstanceId {
				log.Fatalf("inconsitent instance id")
			}
			instance.lock.Lock()
			reply.Na = append(reply.Na, instance.HighestAcN)
			reply.Va = append(reply.Va, instance.HighestAcV)
			instance.lock.Unlock()
		}
	}

	px.varLock.Unlock()

	if reply.OK {
		highestInstanceIdToStore := px.instances.GetHighestIndex()
		px.persistentPrepare(globalNpToStore, highestInstanceIdToStore)
	}

	if ctx != nil && px.afterHandleRPC() {
		return nil, status.Errorf(codes.Unavailable, hitDropReply)
	}
	return reply, nil
}

func (px *Paxos) Accept(ctx context.Context, args *AcceptArgs) (*AcceptReply, error) {
	if ctx != nil && px.beforeHandleRPC(args.Sender) {
		return nil, status.Errorf(codes.Unavailable, hitDropRequest)
	}
	atomic.AddInt64(&px.RpcCount, 1)

	px.peers[args.Sender].lock.Lock()
	px.peers[args.Sender].LastResponseTime = time.Now()
	px.peers[args.Sender].lock.Unlock()

	// must acquire varLock first
	px.varLock.Lock()
	reply := &AcceptReply{}
	if args.N >= px.highestNpSeen {
		px.highestNpSeen = args.N
		reply.OK = true
		if args.InstanceIdTo > px.highestInstanceAccepted {
			px.highestInstanceAccepted = args.InstanceIdTo
		}
	} else {
		reply.OK = false
		utils.Debug("RPC peer %v Accept reply: rejected, N: %v, HighestSeen: ?\n", px.me, args.N)
	}
	globalNpToStore := px.highestNpSeen

	storageLength := int64(0)
	storageContent := new(bytes.Buffer)

	if reply.OK {
		reply.InstanceIdFrom = args.InstanceIdFrom
		if int64(len(args.V)) != args.InstanceIdTo-args.InstanceIdFrom+1 {
			log.Fatalf("wrong len(args.V)")
		}
		for id := args.InstanceIdFrom; id <= args.InstanceIdTo; id++ {
			instance, ok := px.instances.GetAtOrCreateAt(id)
			if !ok {
				reply.InstanceIdFrom = id + 1
				continue
			}
			if id != instance.InstanceId {
				log.Fatalf("inconsitent instance id")
			}
			instance.lock.Lock()
			instance.HighestAcN = args.N
			instance.HighestAcV = args.V[id-args.InstanceIdFrom]

			highestInstanceIdToStore := px.instances.GetHighestIndex()
			length, content := px.encodeInstance(instance, globalNpToStore, highestInstanceIdToStore)
			storageLength += length
			storageContent.Write(content)

			instance.lock.Unlock()
		}
	}
	px.varLock.Unlock()

	if reply.OK && storageLength > 0 {
		storageIns, storageOk := px.instancesToStorageKey.GetAtOrCreateAt(args.InstanceIdTo)
		if !storageOk {
			// it is already freed?
			// for a very very very late Accept request.
		} else {
			storageIns.lock.Lock()
			// in case that we accept an N multiple times or same InstanceIdFrom
			key := "Accept-" + strconv.Itoa(int(args.InstanceIdFrom)) + "-" +
				strconv.Itoa(int(args.InstanceIdTo)) + "-" +
				strconv.Itoa(int(args.N)) + "-" + strconv.Itoa(len(storageIns.tag))
			storageIns.tag = append(storageIns.tag, key)
			err := px.storage.Write(key, storageLength, storageContent.Bytes())
			if err != nil {
				log.Fatalf("cannot write paxos log " + err.Error())
			}
			storageIns.lock.Unlock()
		}
	}

	if ctx != nil && px.afterHandleRPC() {
		return nil, status.Errorf(codes.Unavailable, hitDropReply)
	}
	return reply, nil
}

func (px *Paxos) Decide(ctx context.Context, args *DecideArgs) (*DecideReply, error) {
	if ctx != nil && px.beforeHandleRPC(args.Sender) {
		return nil, status.Errorf(codes.Unavailable, hitDropRequest)
	}
	atomic.AddInt64(&px.RpcCount, 1)

	px.peers[args.Sender].lock.Lock()
	px.peers[args.Sender].LastResponseTime = time.Now()
	px.peers[args.Sender].lock.Unlock()

	for id := args.InstanceIdFrom; id <= args.InstanceIdTo; id++ {
		decidedValue := px.decodeValue(args.V[id-args.InstanceIdFrom])

		instance, ok := px.instances.GetAtOrCreateAt(id)
		if !ok {
			// it is already freed
			continue
		}
		if id != instance.InstanceId {
			log.Fatalf("inconsitent instance id")
		}

		instance.lock.Lock()
		if !instance.Decided {
			instance.Decided = true
			instance.DecidedV = decidedValue
		}
		instance.lock.Unlock()

		// we will only notify once
		instanceId := id
		// do not go here
		func() {
			px.sequentialLock.Lock()
			last := px.lastSequentialInstanceId
			minInstanceId := px.instances.GetFirstIndex()
			if instanceId == last+1 {
				if px.decideSequentialChan != nil {
					//fmt.Printf("id %v sent\n", instanceId)
					px.decideSequentialChan <- DecideEvent{
						MinInstanceId: minInstanceId,
						InstanceId:    instanceId,
						DecidedValue:  decidedValue,
					}
				}
				last = instanceId
				for {
					ins, found := px.instances.GetAt(last + 1)
					if found {
						ins.lock.Lock()
						if ins.Decided {
							last++
						} else {
							ins.lock.Unlock()
							break
						}
						id := ins.InstanceId
						val := ins.DecidedV
						ins.lock.Unlock()
						if px.decideSequentialChan != nil {
							//fmt.Printf("id %v sent\n", id)
							px.decideSequentialChan <- DecideEvent{
								MinInstanceId: minInstanceId,
								InstanceId:    id,
								DecidedValue:  val,
							}
						}
					} else {
						break
					}
				}
				px.lastSequentialInstanceId = last
			}
			px.sequentialLock.Unlock()
		}()
	}

	reply := &DecideReply{}
	reply.OK = true

	if args.Sender != int32(px.me) {
		px.updateMinimumInstanceIdNeeded(int(args.Sender), args.MinimumInstanceId)
		px.cleanDoneInstances()
	}

	if ctx != nil && px.afterHandleRPC() {
		return nil, status.Errorf(codes.Unavailable, hitDropReply)
	}
	return reply, nil
}

func (px *Paxos) Heartbeat(ctx context.Context, args *HeartbeatArgs) (*HeartbeatReply, error) {

	if ctx != nil && atomic.LoadInt32(&px.Unavailable) == 1 {
		return nil, status.Errorf(codes.Unavailable, hitDropRequest)
	}
	if ctx != nil && px.beforeHandleRPC(args.Sender) {
		return nil, status.Errorf(codes.Unavailable, hitDropRequest)
	}

	px.updateMinimumInstanceIdNeeded(int(args.Sender), args.MinimumInstanceId)
	px.instances.GetAtOrCreateAt(args.HighestInstanceIdSeen)

	px.peers[args.Sender].lock.Lock()
	px.peers[args.Sender].LastResponseTime = time.Now()
	px.peers[args.Sender].lock.Unlock()

	leaderNp, _ := px.GetLeaderInfo()
	px.varLock.Lock()
	highestNp := px.highestNpSeen
	px.varLock.Unlock()

	reply := &HeartbeatReply{
		OK:                    true,
		MinimumInstanceId:     px.getMinimumInstanceIdNeeded(px.me),
		HighestInstanceIdSeen: px.Max(),
		LeaderNp:              leaderNp,
		HighestNp:             highestNp,
	}
	return reply, nil
}

type DecideEvent struct {
	MinInstanceId int64
	InstanceId    int64
	DecidedValue  interface{}
}
