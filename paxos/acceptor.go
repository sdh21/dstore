package paxos

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hash/crc32"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
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

const logPrefix = "PAXOSLOG"
const logSuffix = "PAXOSLOGEND"

//  PAXOSLOG  length(4bytes)  crc32(4bytes)   content   PAXOSLOGEND
//  padding to 1KB
//  Content has: instanceId, HighestAcN, HighestAcV, globalNp, highestInstanceIdSeen
//  instance.HighestAcV should already be encoded by encodeValue
func (px *Paxos) encodeInstance(instance *Instance, globalNp int64, instanceIdSeen int64) (int64, []byte) {
	var content bytes.Buffer
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(instanceIdSeen))
	content.Write(buf)
	binary.BigEndian.PutUint64(buf, uint64(globalNp))
	content.Write(buf)
	binary.BigEndian.PutUint64(buf, uint64(instance.InstanceId))
	content.Write(buf)
	binary.BigEndian.PutUint64(buf, uint64(instance.HighestAcN))
	content.Write(buf)
	content.Write(instance.HighestAcV)

	contentBytes := content.Bytes()
	contentLength := len(contentBytes)
	checksum := crc32.ChecksumIEEE(contentBytes)

	resultLength := len(logPrefix) + len(logSuffix) + 8 + contentLength
	padding := 0
	if resultLength%1024 != 0 {
		padding = 1024 - resultLength%1024
	}
	var result bytes.Buffer
	result.Grow(resultLength)
	result.WriteString(logPrefix)
	buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(contentLength))
	result.Write(buf)
	binary.BigEndian.PutUint32(buf, checksum)
	result.Write(buf)
	result.Write(contentBytes)
	result.WriteString(logSuffix)
	if result.Len() != resultLength {
		log.Fatal("impl, paxos.encodeInstance")
	}
	result.Write(make([]byte, padding))
	return int64(resultLength + padding), result.Bytes()
}

func (px *Paxos) decodeInstance(buf []byte) (*Instance, int64, int64, error) {
	index := 0
	if !bytes.Equal(buf[0:len(logPrefix)], []byte(logPrefix)) {
		return nil, 0, 0, errors.New("paxos log corrupted")
	}
	index += len(logPrefix)
	length := int(binary.BigEndian.Uint32(buf[index : index+4]))
	checksum := binary.BigEndian.Uint32(buf[index+4 : index+8])
	index += 8
	if index+length > len(buf) {
		return nil, 0, 0, errors.New("paxos log corrupted")
	}
	content := buf[index : index+length]
	if crc32.ChecksumIEEE(content) != checksum {
		return nil, 0, 0, errors.New("paxos log corrupted")
	}
	index += length
	if !bytes.Equal(buf[index:index+len(logSuffix)], []byte(logSuffix)) {
		return nil, 0, 0, errors.New("paxos log corrupted")
	}
	instance := &Instance{
		Decided:  false,
		DecidedV: nil,
		lock:     sync.Mutex{},
	}
	instanceIdSeen := int64(binary.BigEndian.Uint64(content[0:8]))
	globalNp := int64(binary.BigEndian.Uint64(content[8:16]))
	instance.InstanceId = int64(binary.BigEndian.Uint64(content[16:24]))
	instance.HighestAcN = int64(binary.BigEndian.Uint64(content[24:32]))
	instance.HighestAcV = content[32:]
	return instance, globalNp, instanceIdSeen, nil
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
		key := "Prepare-" + strconv.Itoa(int(args.N))

		highestInstanceIdToStore := px.instances.GetHighestIndex()
		// we only need to store Np; InstanceId=-1 indicates this is a Prepare log
		length, content := px.encodeInstance(&Instance{
			HighestAcN: -1,
			InstanceId: -1,
		}, globalNpToStore, highestInstanceIdToStore)
		err := px.storage.Write(key, length, content)

		if err != nil {
			log.Fatalf("cannot write paxos log " + err.Error())
		}

		// Once we successfully write the new Prepare log, it is safe
		// to delete the old one
		px.prepareNToStorageKeyLock.Lock()
		if px.prepareNToStorageKey != "" {
			err := px.storage.Delete(px.prepareNToStorageKey)
			if err != nil {
				log.Fatalf("cannot delete paxos log " + err.Error())
			}
		}

		px.prepareNToStorageKey = key
		px.prepareNToStorageKeyLock.Unlock()

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
			if instanceId == last+1 {
				if px.decideSequentialChan != nil {
					//fmt.Printf("id %v sent\n", instanceId)
					px.decideSequentialChan <- DecideEvent{
						InstanceId:   instanceId,
						DecidedValue: decidedValue,
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
								InstanceId:   id,
								DecidedValue: val,
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
	InstanceId   int64
	DecidedValue interface{}
}
