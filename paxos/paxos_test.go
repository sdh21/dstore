package paxos

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/sdh21/dstore/utils"
	"log"
	"math"
	"math/rand"
	"net"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// Reference: some tests come from MIT6.824 Paxos Version & Columbia DS courses

const goMAXPROCS = 16

// go test -v -args -useStorage
var UseStorage = flag.Bool("useStorage", false, "")

func createPeers(count int, tag string) ([]*Peer, []net.Listener) {
	peers := make([]*Peer, count)
	listeners := make([]net.Listener, count)
	for i := 0; i < count; i++ {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		peer := &Peer{
			ServerName:        strconv.Itoa(i),
			Address:           l.Addr().String(),
			MinimumInstanceId: 0,
			conn:              nil,
			client:            nil,
		}
		peers[i] = peer
		listeners[i] = l
	}
	return peers, listeners
}

// a storage that does nothing, for testing purpose
type FakeStorage struct {
}

func (fds *FakeStorage) Write(key string, size int64, value []byte) error {
	return nil
}
func (fds *FakeStorage) Delete(key string) error {
	return nil
}

func createPaxos(count int, tag string, t *testing.T) ([]*Peer, []*Paxos) {
	if !*UseStorage {
		return createPaxosNoStorage(count, tag, t)
	} else {
		return createPaxosWithStorage(count, tag, t)
	}
}

func afterTest(pxa []*Paxos, t *testing.T) []*Paxos {
	if !*UseStorage {
		return nil
	} else {
		k := pxRecover(pxa, t)
		fmt.Printf("Storage Test passed.\n")
		return k
	}
}

func createPaxosNoStorage(count int, tag string, t *testing.T) ([]*Peer, []*Paxos) {
	peers, listeners := createPeers(count, tag)
	pxs := make([]*Paxos, count)
	for i := 0; i < count; i++ {
		var err error
		cfg := &ServerConfig{
			Peers:             peers,
			Me:                i,
			Storage:           &FakeStorage{},
			Timeout:           10 * time.Second,
			RandomBackoffMax:  200,
			HeartbeatInterval: 1000,
			LeaderTimeout:     10000,
			LeaderTimeoutRTTs: 10,
		}
		pxs[i], err = NewPaxos(cfg, utils.TestTlsConfig())
		if err != nil {
			t.Fatalf("%v", err)
		}
		pxs[i].l = listeners[i]
		pxs[i].BlockList = make([]int32, count)
		pxs[i].TestEnabled = true
		pxs[i].StartServer()
	}
	return peers, pxs
}

func ndecided(t *testing.T, pxa []*Paxos, instanceId int64) int {
	count := 0
	var v interface{}
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			decided, v1 := pxa[i].GetInstance(instanceId)
			if decided {
				if count > 0 && v != v1 {
					t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
						instanceId, i, v, v1)
				}
				count++
				v = v1
			}
		}
	}
	return count
}

func waitn(t *testing.T, pxa []*Paxos, instanceId int64, wanted int) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if ndecided(t, pxa, instanceId) >= wanted {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	nd := ndecided(t, pxa, instanceId)
	if nd < wanted {
		t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", instanceId, nd, wanted)
	}
}

func waitmajority(t *testing.T, pxa []*Paxos, instanceId int64) {
	waitn(t, pxa, instanceId, (len(pxa)/2)+1)
}

func checkmax(t *testing.T, pxa []*Paxos, instanceId int64, max int) {
	time.Sleep(3 * time.Second)
	nd := ndecided(t, pxa, instanceId)
	if nd > max {
		t.Fatalf("too many decided; seq=%v ndecided=%v max=%v", instanceId, nd, max)
	}
}

func cleanup(pxa []*Paxos) {
	for i := 0; i < len(pxa); i++ {
		if pxa[i] != nil {
			pxa[i].Close()
		}
	}
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "basic", t)

	defer cleanup(pxa)

	fmt.Printf("Test: Single proposer ...\n")

	pxa[0].Propose(0, "hello")
	waitn(t, pxa, 0, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, same value ...\n")

	for i := 0; i < npaxos; i++ {
		pxa[i].Propose(1, 77)
	}
	waitn(t, pxa, 1, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, different values ...\n")

	pxa[0].Propose(2, 100)
	pxa[1].Propose(2, 101)
	pxa[2].Propose(2, 102)
	waitn(t, pxa, 2, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Out-of-order instances ...\n")

	pxa[0].Propose(7, 700)
	pxa[0].Propose(6, 600)
	pxa[1].Propose(5, 500)
	waitn(t, pxa, 7, npaxos)
	pxa[0].Propose(4, 400)
	pxa[1].Propose(3, 300)
	waitn(t, pxa, 6, npaxos)
	waitn(t, pxa, 5, npaxos)
	waitn(t, pxa, 4, npaxos)
	waitn(t, pxa, 3, npaxos)

	if pxa[0].Max() != 7 {
		t.Fatalf("wrong Max()")
	}

	fmt.Printf("  ... Passed\n")

	afterTest(pxa, t)
}

func TestDeaf(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 5
	_, pxa := createPaxos(npaxos, "deaf", t)
	defer cleanup(pxa)

	fmt.Printf("Test: Deaf proposer ...\n")

	pxa[0].Propose(0, "hello")
	waitn(t, pxa, 0, npaxos)

	atomic.StoreInt32(&pxa[0].NotRespondingRPC, 1)
	atomic.StoreInt32(&pxa[npaxos-1].NotRespondingRPC, 1)

	pxa[1].Propose(1, "goodbye")
	waitmajority(t, pxa, 1)
	time.Sleep(1 * time.Second)
	if ndecided(t, pxa, 1) != npaxos-2 {
		t.Fatalf("a deaf peer heard about a decision")
	}

	pxa[0].Propose(1, "xxx")
	waitn(t, pxa, 1, npaxos-1)
	time.Sleep(1 * time.Second)
	if ndecided(t, pxa, 1) != npaxos-1 {
		t.Fatalf("a deaf peer heard about a decision")
	}

	pxa[npaxos-1].Propose(1, "yyy")
	waitn(t, pxa, 1, npaxos)

	fmt.Printf("  ... Passed\n")

	afterTest(pxa, t)
}

func TestSpeed(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "speed", t)
	defer cleanup(pxa)

	t0 := time.Now()

	for i := int64(0); i < 2000; i++ {
		pxa[0].Propose(i, "x")
		//waitn(t, pxa, i, npaxos)
	}
	for i := int64(0); i < 2000; i++ {
		waitn(t, pxa, i, npaxos)
	}
	d := time.Since(t0)
	fmt.Printf("2000 agreements %v seconds\n", d.Seconds())

	afterTest(pxa, t)
}

func TestSpeedHighLatency(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "speed-hl", t)
	defer cleanup(pxa)

	// 200ms RTT
	for _, px := range pxa {
		atomic.StoreInt32(&px.LatencyLowerBound, 100)
		atomic.StoreInt32(&px.LatencyUpperBound, 101)
	}

	t0 := time.Now()

	for i := int64(0); i < 20; i++ {
		pxa[0].Propose(i, "x")
		waitn(t, pxa, i, npaxos)
	}

	d := time.Since(t0)
	fmt.Printf("20 agreements %v seconds\n", d.Seconds())

	afterTest(pxa, t)
}

func TestSpeedHighLatencyConcurrentPropose(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "speed-hlconcu", t)
	defer cleanup(pxa)

	// 200ms RTT
	for _, px := range pxa {
		atomic.StoreInt32(&px.LatencyLowerBound, 100)
		atomic.StoreInt32(&px.LatencyUpperBound, 101)
	}

	t0 := time.Now()

	pxa[0].Propose(0, "x")
	waitn(t, pxa, 0, npaxos)

	for i := int64(1); i < 2000; i++ {
		pxa[0].Propose(i, "x")
	}

	for i := int64(0); i < 2000; i++ {
		waitn(t, pxa, i, npaxos)
	}

	d := time.Since(t0)
	fmt.Printf("2000 agreements %v seconds\n", d.Seconds())

	afterTest(pxa, t)
}

func TestManyProposers(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 5
	_, pxa := createPaxos(npaxos, "multipaxos-manyproposers", t)
	defer cleanup(pxa)

	t0 := time.Now()

	for i := int64(0); i < 2000; i++ {
		pxa[0].Propose(i, "x")
		pxa[1].Propose(i, "y")
		pxa[2].Propose(i, "z")
		pxa[3].Propose(i, "a")
		pxa[4].Propose(i, "b")
	}
	for {
		done := true
		for seq := int64(0); seq < 2000; seq++ {
			if ndecided(t, pxa, int64(seq)) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	d := time.Since(t0)
	fmt.Printf("2000 agreements %v seconds\n", d.Seconds())

	afterTest(pxa, t)
}

func setFloat64(f *float64, v float64) {
	vf := math.Float64bits(v)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(f)), vf)
}

func TestManyProposersUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "multipaxos-manyproposers-unreliable", t)
	defer cleanup(pxa)

	for _, px := range pxa {
		atomic.StoreInt32(&px.Unreliable, 1)
		setFloat64(&px.DropReplyProbability, 0.3)
		setFloat64(&px.DropRequestProbability, 0.2)
	}

	t0 := time.Now()

	ninst := int64(2000)
	for i := int64(0); i < ninst; i++ {
		pxa[0].Propose(i, "x")
		pxa[1].Propose(i, "y")
		pxa[2].Propose(i, "z")
	}

	for {
		done := true
		for seq := int64(0); seq < ninst; seq++ {
			if ndecided(t, pxa, int64(seq)) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	d := time.Since(t0)
	fmt.Printf("%v agreements %v seconds\n", ninst, d.Seconds())

	afterTest(pxa, t)
}

func TestManyProposersChaotic(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "multipaxos-manyproposers-chaotic", t)
	defer cleanup(pxa)

	for _, px := range pxa {
		atomic.StoreInt32(&px.LatencyLowerBound, 0)
		atomic.StoreInt32(&px.LatencyUpperBound, 12)
	}

	go func() {
		for {
			for _, px := range pxa {
				atomic.StoreInt32(&px.Unreliable, 1)
				setFloat64(&px.DropReplyProbability, rand.Float64()/2)
				setFloat64(&px.DropRequestProbability, rand.Float64()/2)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	ninst := int64(50)
	t0 := time.Now()

	for i := int64(0); i < ninst; i++ {
		pxa[0].Propose(i, "x")
		pxa[1].Propose(i, "y")
		pxa[2].Propose(i, "z")
	}

	for {
		done := true
		for seq := int64(0); seq < ninst; seq++ {
			if ndecided(t, pxa, int64(seq)) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	d := time.Since(t0)
	fmt.Printf("%v agreements %v seconds\n", ninst, d.Seconds())

	afterTest(pxa, t)
}

func TestMultiPaxosLeader(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)
	const npaxos = 3
	_, pxa := createPaxos(npaxos, "multipaxos-leader", t)
	defer cleanup(pxa)

	ninst := int64(2000)

	// wait for a leader
	leader := -1
	for {
		for _, px := range pxa {
			j, _ := px.GetLeaderInfo()
			if j != -1 {
				leader = int(j)
				break
			}
		}
		if leader != -1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	t0 := time.Now()
	for i := int64(1); i < ninst; i++ {
		pxa[leader].Propose(i, "x"+strconv.Itoa(int(i)))
	}

	for peer := 0; peer < npaxos; peer++ {
		for i := int64(0); i < ninst; i++ {
			for {
				ok, _ := pxa[peer].GetInstance(int64(i))
				if !ok {
					time.Sleep(100 * time.Millisecond)
				} else {
					break
				}
			}
		}
	}

	d := time.Since(t0)

	if int(pxa[leader].SkippedPhase1Count) != int(ninst-1) {
		t.Fatalf("skippedPhase1Count, %v found, %v expected",
			pxa[leader].SkippedPhase1Count, ninst-1)
	}

	// the leader does not have its local rpc counted
	for i := 0; i < len(pxa); i++ {
		if i == leader {
			continue
		}
		px := pxa[i]
		if px.RpcCount > int64(2*(ninst)+1) {
			t.Fatalf("too much rpc, %v found, %v expected",
				px.RpcCount, int(2*(ninst)+1))
		}
	}

	fmt.Printf("%v agreements %v seconds\n", ninst, d.Seconds())

	afterTest(pxa, t)
}

func TestMultiPaxosLeaderSwitch(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)
	const npaxos = 3
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	// make 0 as leader
	pxa[0].Propose(0, "x")
	waitn(t, pxa, 0, npaxos)

	n1, ins := pxa[0].GetLeaderInfo()
	if ins != 1 {
		t.Fatalf("wrong term")
	}

	pxa[1].Propose(1, "y")
	waitn(t, pxa, 1, npaxos)
	_, ins = pxa[1].GetLeaderInfo()
	if ins != 2 {
		t.Fatalf("wrong term")
	}

	pxa[0].Propose(2, "x")
	waitn(t, pxa, 2, npaxos)
	n2, ins2 := pxa[0].GetLeaderInfo()
	if n1 == n2 || ins2 != 3 {
		t.Fatalf("wrong term")
	}
	fmt.Printf("%v %v\n", n1, n2)

	afterTest(pxa, t)
}

func part(t *testing.T, pxs []*Paxos, p ...[]int) {
	for i := 0; i < len(pxs); i++ {
		for j := 0; j < len(pxs); j++ {
			atomic.StoreInt32(&pxs[i].TestParams.BlockList[j], 1)
		}
	}

	// For servers in the same partition:
	for pi := 0; pi < len(p); pi++ {
		for _, pa := range p[pi] {
			for _, pb := range p[pi] {
				atomic.StoreInt32(&pxs[pa].TestParams.BlockList[pb], 0)
			}
		}
	}
}

func TestPartition(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)
	const npaxos = 5
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: No decision if partitioned ...\n")
	part(t, pxa, []int{0, 2}, []int{1, 3}, []int{4})

	pxa[1].Propose(0, 111)
	checkmax(t, pxa, 0, 0)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Decision in majority partition ...\n")
	part(t, pxa, []int{0}, []int{1, 2, 3}, []int{4})

	time.Sleep(2 * time.Second)
	waitmajority(t, pxa, 0)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: All agree after full heal ...\n")
	part(t, pxa, []int{0, 1, 2, 3, 4})

	waitn(t, pxa, 0, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: One peer switches partitions ...\n")

	seq := int64(0)
	for iters := 0; iters < 20; iters++ {
		seq++
		part(t, pxa, []int{0, 1, 2}, []int{3, 4})

		pxa[0].Propose(seq, seq*10)
		pxa[3].Propose(seq, (seq*10)+1)
		waitmajority(t, pxa, seq)
		if ndecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}
		//fmt.Printf("  {2,3,4}\n")
		pxa[0].proposeLock.Lock()
		pxa[3].proposeLock.Lock()
		part(t, pxa, []int{0, 1}, []int{2, 3, 4})
		pxa[0].proposeLock.Unlock()
		pxa[3].proposeLock.Unlock()
		waitn(t, pxa, seq, npaxos)
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

	for iters := 0; iters < 20; iters++ {
		seq++
		for i := 0; i < npaxos; i++ {
			atomic.StoreInt32(&pxa[i].Unreliable, 1)
			setFloat64(&pxa[i].DropReplyProbability, 0.2)
			setFloat64(&pxa[i].DropRequestProbability, 0.22)
		}
		part(t, pxa, []int{0, 1, 2}, []int{3, 4})

		for i := 0; i < npaxos; i++ {
			pxa[i].Propose(seq, (seq*10)+int64(i))
		}
		waitn(t, pxa, seq, 3)
		if ndecided(t, pxa, seq) > 3 {
			t.Fatalf("too many decided")
		}

		part(t, pxa, []int{0, 1}, []int{2, 3, 4})

		for i := 0; i < npaxos; i++ {
			atomic.StoreInt32(&pxa[i].Unreliable, 0)
		}
		waitn(t, pxa, seq, npaxos)
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestPartitionChaotic(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)
	const npaxos = 5
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: Many requests, changing partitions ...\n")
	for i := 0; i < npaxos; i++ {
		atomic.StoreInt32(&pxa[i].Unreliable, 1)
		setFloat64(&pxa[i].DropReplyProbability, 0.2)
		setFloat64(&pxa[i].DropRequestProbability, 0.22)
	}
	// re-partition periodically
	ch1 := make(chan bool)
	done := false
	go func() {
		defer func() { ch1 <- true }()
		for !done {
			for i := 0; i < npaxos; i++ {
				for j := 0; j < npaxos; j++ {
					if i < j {
						atomic.StoreInt32(&pxa[i].BlockList[j], int32(rand.Intn(2)))
						atomic.StoreInt32(&pxa[j].BlockList[i], int32(rand.Intn(2)))
					}
				}
			}
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := 0

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() { ch2 <- true }()
		for done == false {
			// how many instances are in progress?
			nd := 0
			for i := 0; i < seq; i++ {
				if ndecided(t, pxa, int64(i)) == npaxos {
					nd++
				}
			}
			if seq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					pxa[i].Propose(int64(seq), rand.Int()%10)
				}
				seq++
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for done == false {
			for i := 0; i < seq; i++ {
				ndecided(t, pxa, int64(i))
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	done = true
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	for i := 0; i < npaxos; i++ {
		atomic.StoreInt32(&pxa[i].Unreliable, 0)
	}

	for i := 0; i < npaxos; i++ {
		for j := 0; j < npaxos; j++ {
			atomic.StoreInt32(&pxa[i].BlockList[j], 0)
		}
	}

	time.Sleep(5 * time.Second)

	for i := 0; i < seq; i++ {
		waitn(t, pxa, int64(i), npaxos)
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestPartitionChaotic2(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)
	const npaxos = 5
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: Many requests, changing partitions ...\n")
	for i := 0; i < npaxos; i++ {
		atomic.StoreInt32(&pxa[i].Unreliable, 1)
		setFloat64(&pxa[i].DropReplyProbability, 0.2)
		setFloat64(&pxa[i].DropRequestProbability, 0.22)
	}
	// re-partition periodically
	ch1 := make(chan bool)
	done := false
	go func() {
		defer func() { ch1 <- true }()
		for !done {
			for i := 0; i < npaxos; i++ {
				for j := 0; j < npaxos; j++ {
					if i != j {
						atomic.StoreInt32(&pxa[i].BlockList[j], int32(rand.Intn(2)))
					}
				}
			}
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := 0

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() { ch2 <- true }()
		for done == false {
			// how many instances are in progress?
			nd := 0
			for i := 0; i < seq; i++ {
				if ndecided(t, pxa, int64(i)) == npaxos {
					nd++
				}
			}
			if seq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					pxa[i].Propose(int64(seq), rand.Int()%10)
				}
				seq++
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for done == false {
			for i := 0; i < seq; i++ {
				ndecided(t, pxa, int64(i))
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	done = true
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	for i := 0; i < npaxos; i++ {
		atomic.StoreInt32(&pxa[i].Unreliable, 0)
	}

	for i := 0; i < npaxos; i++ {
		for j := 0; j < npaxos; j++ {
			atomic.StoreInt32(&pxa[i].BlockList[j], 0)
		}
	}

	time.Sleep(5 * time.Second)

	for i := 0; i < seq; i++ {
		waitn(t, pxa, int64(i), npaxos)
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestForget(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 6
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: Forgetting ...\n")

	// initial Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m > 0 {
			t.Fatalf("wrong initial Min() %v", m)
		}
	}

	pxa[0].Propose(0, "00")
	pxa[1].Propose(1, "11")
	pxa[2].Propose(2, "22")
	pxa[0].Propose(6, "66")
	pxa[1].Propose(7, "77")

	waitn(t, pxa, 0, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	waitn(t, pxa, 1, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	// everyone Done() -> Min() changes?
	for i := 0; i < npaxos; i++ {
		pxa[i].Done(0)
	}
	for i := 1; i < npaxos; i++ {
		pxa[i].Done(1)
	}
	allok := false
	for iters := 0; iters < 12; iters++ {
		allok = true
		for i := 0; i < npaxos; i++ {
			s := pxa[i].Min()
			if s != 1 {
				allok = false
			}
		}
		if allok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if allok != true {
		t.Fatalf("Min() did not advance after Done()")
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestManyForget(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: Lots of forgetting ...\n")

	const maxseq = 2000
	done := false

	go func() {
		na := rand.Perm(maxseq)
		for i := 0; i < len(na); i++ {
			seq := na[i]
			j := rand.Int() % npaxos
			v := rand.Int()
			pxa[j].Propose(int64(seq), v)
			runtime.Gosched()
		}
	}()

	go func() {
		for done == false {
			seq := rand.Int() % maxseq
			i := rand.Int() % npaxos
			if int64(seq) >= pxa[i].Min() {
				decided, _ := pxa[i].GetInstance(int64(seq))
				if decided {
					pxa[i].Done(int64(seq))
				}
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(5 * time.Second)
	done = true
	for i := 0; i < npaxos; i++ {
		atomic.StoreInt32(&pxa[i].Unreliable, 1)
	}
	time.Sleep(2 * time.Second)

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestTakeover(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: A server with many missing logs is elected as leader, and quickly catch up logs ...\n")

	for {
		// wait for the leader
		leaderNp, _ := pxa[0].GetLeaderInfo()
		if leaderNp != -1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// make server 1 unavailable
	atomic.StoreInt32(&pxa[1].Unavailable, 1)

	t0 := time.Now()
	proposalCount := 40000
	for i := 0; i < proposalCount; i++ {
		pxa[0].Propose(int64(i), strconv.Itoa(i)+"12345678")
	}

	for i := 0; i < proposalCount; i++ {
		for {
			if ndecided(t, pxa, int64(i)) >= npaxos-1 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	d := time.Since(t0)
	fmt.Printf("%v agreements %v seconds\n", proposalCount, d.Seconds())

	// server 1 should know nothing about proposals

	if pxa[1].instances.Size() != 1 {
		// 1 log is the election log
		t.Fatalf("server 1 should know nothing about proposals")
	}

	// 1 not receiving 0's decides
	atomic.StoreInt32(&pxa[1].BlockList[0], 1)
	atomic.StoreInt32(&pxa[1].Unavailable, 0)
	time.Sleep(time.Duration(pxa[1].heartbeatInterval*5) * time.Millisecond)

	// no heartbeat from 0
	atomic.StoreInt32(&pxa[0].Unavailable, 1)

	t1 := time.Now()

	fmt.Printf("waiting for the new leader 1\n")
	rpc := pxa[1].RpcCount
	for {
		// wait for the new leader
		leaderNp, _ := pxa[1].GetLeaderInfo()
		if leaderNp != -1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < proposalCount; i++ {
		for {
			ok, _ := pxa[1].GetInstance(int64(i))
			if !ok {
				time.Sleep(100 * time.Millisecond)
			} else {
				break
			}
		}
	}
	fmt.Printf("time to recover: %v seconds\n", time.Since(t1).Seconds())

	for i := 0; i < proposalCount; i++ {
		for {
			ok, value := pxa[1].GetInstance(int64(i))
			if !ok {
				t.Fatalf("cannot happen")
			} else {
				_, v0 := pxa[0].GetInstance(int64(i))
				_, v2 := pxa[2].GetInstance(int64(i))
				if !reflect.DeepEqual(value, v0) ||
					!reflect.DeepEqual(value, v2) {
					t.Fatalf("decided value inconsistent")
				}
				break
			}
		}
	}

	// how many rpc used?
	rpc2 := pxa[1].RpcCount
	fmt.Printf("RPC used: %v-%v=%v\n", rpc2, rpc, rpc2-rpc)
	if rpc2-rpc > 3 {
		t.Fatalf("too many RPC used to catch up")
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestProposeValue(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	t0 := time.Now()

	proposalCount := 40000
	for i := 0; i < proposalCount; {
		result := pxa[0].ProposeValue(strconv.Itoa(i)+"12345678", func(i interface{}, i2 interface{}) bool {
			return i.(string) == i2.(string)
		})
		if !result.Succeeded {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("waiting for leader!\n")
			continue
		}
		i++
	}

	for i := 0; i < proposalCount; i++ {
		for {
			if ndecided(t, pxa, int64(i)) >= npaxos {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	for i := 0; i < proposalCount; i++ {
		for {
			ok, value := pxa[1].GetInstance(int64(i))
			if !ok {
				t.Fatalf("cannot happen")
			} else {
				_, v0 := pxa[0].GetInstance(int64(i))
				_, v2 := pxa[2].GetInstance(int64(i))
				if !reflect.DeepEqual(value, v0) ||
					!reflect.DeepEqual(value, v2) {
					t.Fatalf("decided value inconsistent")
				}
				break
			}
		}
	}

	d := time.Since(t0)
	fmt.Printf("%v agreements %v seconds\n", proposalCount, d.Seconds())

	//m := runtime.MemStats{}
	//runtime.ReadMemStats(&m)

	if pxa[0].SkippedPhase1Count < proposalCount {
		t.Fatalf("not fast path, SkippedPhase1Count: %v, proposalCount: %v",
			pxa[0].SkippedPhase1Count, proposalCount)
	}

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}

func TestDoneCollector(t *testing.T) {
	runtime.GOMAXPROCS(goMAXPROCS)

	const npaxos = 3
	_, pxa := createPaxos(npaxos, "", t)
	defer cleanup(pxa)

	fmt.Printf("Test: the done instances collector frees Paxos log memory...\n")

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	objectSize := 100000

	v2 := make([]byte, objectSize)
	for j := 0; j < len(v2); j++ {
		v2[j] = byte((rand.Int() % 100) + 32)
	}
	var wg sync.WaitGroup
	for m := 0; m < 40; m++ {
		for k := 0; k < 100; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				value := make([]byte, objectSize)
				copy(value, v2)
				result := pxa[0].ProposeValue(value,
					func(i interface{}, i2 interface{}) bool {
						return bytes.Equal(i.([]byte), i2.([]byte))
					})
				for _, px := range pxa {
					px.Done(result.InstanceId)
				}

			}()
			wg.Wait()
			runtime.GC()
			time.Sleep(5 * time.Millisecond)
		}
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	fmt.Printf("  Memory: before %v, after %v\n", m0.Alloc, m1.Alloc)

	fmt.Printf("  ... Passed\n")
	afterTest(pxa, t)
}
