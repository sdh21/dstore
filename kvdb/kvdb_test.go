package kvdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/utils"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// Reference: some tests come from MIT6.824 Paxos Version & CU DS courses

func check(t *testing.T, ck *Client, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func createPeers(count int) ([]*paxos.Peer, []net.Listener) {
	peers := make([]*paxos.Peer, count)
	listeners := make([]net.Listener, count)
	for i := 0; i < count; i++ {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		peer := &paxos.Peer{
			ServerName:        strconv.Itoa(i),
			Address:           l.Addr().String(),
			MinimumInstanceId: 0,
		}
		peers[i] = peer
		listeners[i] = l
	}
	return peers, listeners
}

func createServers(t *testing.T, count int) ([]*KeyValueDB, []string) {
	peers, listeners := createPeers(count)
	servers := make([]*KeyValueDB, 0)
	addrs := make([]string, 0)
	for i := 0; i < count; i++ {
		fo := "/tmp/kvdb_test/db-" + strconv.Itoa(i)
		os.RemoveAll(fo + "/paxos")
		os.MkdirAll(fo+"/paxos", 0777)
		os.RemoveAll(fo + "/checkpoint")
		os.MkdirAll(fo+"/checkpoint/data-blocks", 0777)
		os.MkdirAll(fo+"/checkpoint/file-metadata-blocks", 0777)
		config := DefaultConfig()
		config.StorageFolder = fo
		config.PaxosConfig.Listener = listeners[i]
		config.PaxosConfig.Peers = peers
		config.PaxosConfig.Me = i
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		config.DBAddress = l.Addr().String()
		server, err := NewServer(config, utils.TestTlsConfig())
		if err != nil {
			t.Fatalf("error: %v", e)
		}
		server.l = l
		server.px.TestEnabled = true
		server.px.BlockList = make([]int32, count)
		servers = append(servers, server)
		addrs = append(addrs, config.DBAddress)
	}
	for i := 0; i < count; i++ {
		servers[i].addresses = addrs
	}
	return servers, addrs
}

func startServer(t *testing.T, servers []*KeyValueDB) {
	for _, s := range servers {
		s.StartServer()
	}
}

func cleanup(kva []*KeyValueDB) {
	for i := 0; i < len(kva); i++ {
		if kva[i] != nil {
			kva[i].Close()
		}
	}
}

func checkPaxos(t *testing.T, servers []*KeyValueDB) {
	maxi := int64(0)
	for _, server := range servers {
		if server.px.Max() > maxi {
			maxi = server.px.Max()
		}
	}

	for i := int64(0); i <= maxi; i++ {
		var vd interface{}
		vdset := false
		for _, server := range servers {
			decided, v := server.px.GetInstance(i)
			if decided {
				if !vdset {
					vdset = true
					vd = v
				} else {
					v1, _ := json.Marshal(vd)
					v2, _ := json.Marshal(v)
					if !bytes.Equal(v1, v2) {
						t.Fatalf("not consistent, got %v,\n want %v", string(v1),
							string(v2))
					}
				}
			}
		}
	}
}

func checkConsistency(t *testing.T, servers []*KeyValueDB) {
	checkPaxos(t, servers)

	// reset server status
	for _, server := range servers {
		server.px.TestEnabled = false
		server.px.Unavailable = 0
		server.px.Unreliable = 0
		server.px.BlockList = make([]int32, len(servers))
	}

	for _, server := range servers {
		// filling in holes
		for {
			result := server.px.ForceElectMe()
			if result.Succeeded {
				break
			}
			fmt.Printf("ForceElectMe failed with %v\n", result.Reason)
			time.Sleep(100 * time.Millisecond)
		}
	}
	// waiting for log applying
	for {
		last := int64(0)
		for _, server := range servers {
			if server.px.Max() > last {
				last = server.px.Max()
			}
		}
		checkPaxos(t, servers)

		ok := true
		for _, server := range servers {
			if last != int64(server.lastProposalId) {
				ok = false
			}
		}
		if ok {
			break
		}
	}

	for _, server := range servers {
		v, _ := json.Marshal(server.state.tables)
		v2, _ := json.Marshal(servers[0].state.tables)
		if !bytes.Equal(v, v2) {
			t.Fatalf("not consistent, got %v,\n want %v", string(v),
				string(v2))
		}
		waitingCount := atomic.LoadInt64(&server.waitingCount)
		if waitingCount > 0 {
			t.Fatalf("BatchSubmit rpc not returned, wc: %v", waitingCount)
		}
	}

	checkPaxos(t, servers)
}

const goMaxProcs = 16

func newClient(s []string, id int64) *Client {
	return NewClient(s, id, utils.TestTlsConfig())
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 3
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)

	var clientId int64 = 1

	client := newClient(addresses, atomic.AddInt64(&clientId, 1))

	startServer(t, servers)

	fmt.Printf("Test: Basic put/puthash/get ...\n")

	pv := client.PutHash("a", "x")
	ov := ""
	if ov != pv {
		t.Fatalf("wrong value; expected %s got %s", ov, pv)
	}

	client.Put("a", "aa")
	check(t, client, "a", "aa")

	client.Put("a", "aaa")

	check(t, client, "a", "aaa")
	check(t, client, "a", "aaa")
	check(t, client, "a", "aaa")

	client.Put("b", "ababbce")
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients ...\n")

	const nclients = 15
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	for iters := 0; iters < 20; iters++ {
		var ca [nclients]chan bool
		for nth := 0; nth < nclients; nth++ {
			ca[nth] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				cl := clients[me]
				if (rand.Int() % 1000) < 500 {
					cl.Put("b", "ababbce")
				} else {
					if cl.Get("b") != "ababbce" {
						t.Fatalf("not match")
					}
				}
			}(nth)
		}
		for nth := 0; nth < nclients; nth++ {
			<-ca[nth]
		}
		if client.Get("b") != "ababbce" {
			t.Fatalf("not match")
		}
	}

	fmt.Printf("  ... Passed\n")

	checkConsistency(t, servers)
}

func cleanpp(t *testing.T, pxs []*KeyValueDB) {
	for i := 0; i < len(pxs); i++ {
		for j := 0; j < len(pxs); j++ {
			if !pxs[i].px.TestEnabled {
				continue
			}
			atomic.StoreInt32(&pxs[i].px.TestParams.BlockList[j], 0)
		}
	}
}

func part(t *testing.T, pxs []*KeyValueDB, p ...[]int) {
	for i := 0; i < len(pxs); i++ {
		for j := 0; j < len(pxs); j++ {
			atomic.StoreInt32(&pxs[i].px.TestParams.BlockList[j], 1)
		}
	}

	// For servers in the same partition:
	for pi := 0; pi < len(p); pi++ {
		for _, pa := range p[pi] {
			for _, pb := range p[pi] {
				atomic.StoreInt32(&pxs[pa].px.TestParams.BlockList[pb], 0)
			}
		}
	}
}

func TestPartition(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 5
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)
	defer cleanpp(t, servers)

	var clientId int64 = 1

	client := newClient(addresses, clientId)
	atomic.AddInt64(&clientId, 1)

	startServer(t, servers)

	fmt.Printf("Test: No partition ...\n")

	part(t, servers, []int{0, 1, 2, 3, 4})
	client.Put("1", "12")
	client.Put("1", "13")
	check(t, client, "1", "13")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Progress in majority ...\n")

	part(t, servers, []int{2, 3, 4}, []int{0, 1})
	client.Put("1", "14")
	check(t, client, "1", "14")

	fmt.Printf("  ... Passed\n")

	part(t, servers, []int{2, 1}, []int{0, 3}, []int{4})

	fmt.Printf("Test: No progress if no majority...\n")

	done0 := false
	done1 := false
	go func() {
		client.Put("1", "15")
		done0 = true
	}()
	go func() {
		client.Get("1")
		done1 = true
	}()
	time.Sleep(5 * time.Second)
	if done0 {
		t.Fatalf("Put in minority completed")
	}
	if done1 {
		t.Fatalf("Get in minority completed")
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	part(t, servers, []int{0, 2, 3, 4}, []int{1}, []int{})
	for iters := 0; iters < 30; iters++ {
		if done0 && done1 {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	if done0 == false {
		t.Fatalf("Put did not complete")
	}
	if done1 == false {
		t.Fatalf("Get did not complete")
	}
	check(t, client, "1", "15")

	fmt.Printf("  ... Passed\n")
	checkConsistency(t, servers)
}

func NextValue(hprev string, val string) string {
	h := hash(hprev + val)
	return strconv.Itoa(int(h))
}

func TestPartitionPutHash(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 5
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)
	defer cleanpp(t, servers)

	var clientId int64 = 1

	const nclients = 10
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	startServer(t, servers)

	fmt.Printf("Test: No partition, concurrent clients ...\n")

	part(t, servers, []int{0, 1, 2, 3, 4})

	var wg, wg2, wg3, wg4, wg5 sync.WaitGroup
	for i, _ := range clients {
		client := clients[i]
		key := "a" + strconv.Itoa(i)
		prev := client.Get(key)
		wg.Add(1)
		wg2.Add(1)
		wg3.Add(1)
		wg4.Add(1)
		wg5.Add(1)
		go func() {
			for k := 0; k < 2000; k++ {
				if k == 300 {
					wg.Done()
				} else if k == 600 {
					wg2.Done()
				} else if k == 900 {
					wg3.Done()
				} else if k == 1200 {
					wg4.Done()
				}
				value := strconv.Itoa(rand.Int())
				next := NextValue(prev, value)
				client.PutHash(key, value)
				prev = client.Get(key)
				if prev != next {
					t.Fatalf("wrong value; %s expected, but got %s", next, prev)
				}
			}
			wg5.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Progress in majority ...\n")

	part(t, servers, []int{2, 3, 4}, []int{0, 1})
	wg2.Wait()

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: NO Progress in minority ...\n")
	part(t, servers, []int{2, 1}, []int{0, 3}, []int{4})
	time.Sleep(10 * time.Second)
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	part(t, servers, []int{4, 3, 0}, []int{2, 1})
	wg3.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Partitioned again ...\n")
	part(t, servers, []int{2, 3, 4}, []int{0, 1})
	wg4.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after full heal ...\n")

	part(t, servers, []int{4, 3, 0, 2, 1})
	wg5.Wait()
	fmt.Printf("  ... Passed\n")

	checkConsistency(t, servers)
}

func setFloat64(f *float64, v float64) {
	vf := math.Float64bits(v)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(f)), vf)
}

func TestPartitionPutHashChaotic(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 5
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)
	defer cleanpp(t, servers)

	var clientId int64 = 1

	const nclients = 100
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	for _, server := range servers {
		atomic.StoreInt32(&server.px.Unreliable, 1)
		setFloat64(&server.px.DropReplyProbability, 0.1)
		setFloat64(&server.px.DropRequestProbability, 0.1)
		atomic.StoreInt32(&server.px.LatencyLowerBound, 1)
		atomic.StoreInt32(&server.px.LatencyUpperBound, 10)
	}

	startServer(t, servers)

	fmt.Printf("Test: No partition, concurrent clients ...\n")

	part(t, servers, []int{0, 1, 2, 3, 4})

	var wg, wg2, wg3, wg4, wg5 sync.WaitGroup
	for i, _ := range clients {
		client := clients[i]
		key := "a" + strconv.Itoa(i)
		prev := client.Get(key)
		wg.Add(1)
		wg2.Add(1)
		wg3.Add(1)
		wg4.Add(1)
		wg5.Add(1)
		go func() {
			for k := 0; k < 140; k++ {
				if k == 30 {
					wg.Done()
				} else if k == 60 {
					wg2.Done()
				} else if k == 90 {
					wg3.Done()
				} else if k == 120 {
					wg4.Done()
				}
				value := strconv.Itoa(rand.Int())
				next := NextValue(prev, value)
				client.PutHash(key, value)
				prev = client.Get(key)
				if prev != next {
					t.Fatalf("wrong value; %s expected, but got %s", next, prev)
				}
			}
			wg5.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Progress in majority ...\n")

	part(t, servers, []int{2, 3, 4}, []int{0, 1})
	wg2.Wait()

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: NO Progress in minority ...\n")
	part(t, servers, []int{2, 1}, []int{0, 3}, []int{4})
	time.Sleep(10 * time.Second)
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	part(t, servers, []int{4, 3, 0}, []int{2, 1})
	wg3.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Partitioned again ...\n")
	part(t, servers, []int{2, 3, 4}, []int{0, 1})
	wg4.Wait()
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after full heal ...\n")

	part(t, servers, []int{4, 3, 0, 2, 1})
	wg5.Wait()
	fmt.Printf("  ... Passed\n")

	checkConsistency(t, servers)
}

func TestUnstable(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 5
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)
	defer cleanpp(t, servers)

	var clientId int64 = 1

	const nclients = 10
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	for _, server := range servers {
		atomic.StoreInt32(&server.px.Unreliable, 1)
		setFloat64(&server.px.DropReplyProbability, 0.3)
		setFloat64(&server.px.DropRequestProbability, 0.3)
		atomic.StoreInt32(&server.px.LatencyLowerBound, 1)
		atomic.StoreInt32(&server.px.LatencyUpperBound, 10)
	}

	//   a server suddenly has poor network
	go func() {
		for {
			whichServer := rand.Intn(len(servers))
			fmt.Printf("%v becomes unstable\n", whichServer)
			server := servers[whichServer]
			setFloat64(&server.px.DropReplyProbability, 0.60)
			setFloat64(&server.px.DropRequestProbability, 0.60)
			atomic.StoreInt32(&server.px.LatencyLowerBound, 10)
			atomic.StoreInt32(&server.px.LatencyUpperBound, 250)
			time.Sleep(5000 * time.Millisecond)
			atomic.StoreInt32(&server.px.Unreliable, 1)
			setFloat64(&server.px.DropReplyProbability, 0.3)
			setFloat64(&server.px.DropRequestProbability, 0.3)
			atomic.StoreInt32(&server.px.LatencyLowerBound, 1)
			atomic.StoreInt32(&server.px.LatencyUpperBound, 10)
			fmt.Printf("%v recovered\n", whichServer)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	startServer(t, servers)

	fmt.Printf("Test: Random server becomes unstable ...\n")

	var wg sync.WaitGroup
	for i, _ := range clients {
		client := clients[i]
		key := "a" + strconv.Itoa(i)
		prev := client.Get(key)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				value := strconv.Itoa(rand.Int())
				next := NextValue(prev, value)
				client.PutHash(key, value)
				prev = client.Get(key)
				if prev != next {
					t.Fatalf("wrong value; %s expected, but got %s", next, prev)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("  ... Passed\n")

	checkConsistency(t, servers)
}

func TestUnavailable(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 5
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)
	defer cleanpp(t, servers)

	var clientId int64 = 1

	const nclients = 100
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	for _, server := range servers {
		atomic.StoreInt32(&server.px.Unreliable, 1)
		setFloat64(&server.px.DropReplyProbability, 0.05)
		setFloat64(&server.px.DropRequestProbability, 0.05)
		atomic.StoreInt32(&server.px.LatencyLowerBound, 1)
		atomic.StoreInt32(&server.px.LatencyUpperBound, 10)
	}

	done := int32(0)
	var wgTest sync.WaitGroup
	wgTest.Add(1)
	//   leader suddenly becomes unavailable
	go func() {
		defer wgTest.Done()
		for {
			if atomic.LoadInt32(&done) == 1 {
				break
			}
			time.Sleep(3500 * time.Millisecond)
			if atomic.LoadInt32(&done) == 1 {
				break
			}
			var whichServer *KeyValueDB
			np := int64(-1)
			for _, s := range servers {
				leaderNp, _ := s.px.GetLeaderInfo()
				if leaderNp > np {
					np = leaderNp
					whichServer = s
				}
			}
			// where is the leader?
			if whichServer == nil {
				continue
			}
			atomic.StoreInt32(&whichServer.px.Unavailable, 1)
			time.Sleep(5000 * time.Millisecond)
			atomic.StoreInt32(&whichServer.px.Unavailable, 0)
		}
	}()

	startServer(t, servers)

	fmt.Printf("Test: Random server becomes unavailable ...\n")
	var wg sync.WaitGroup
	for i, _ := range clients {
		client := clients[i]
		key := "a" + strconv.Itoa(i)
		prev := client.Get(key)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				value := strconv.Itoa(rand.Int())
				next := NextValue(prev, value)
				client.PutHash(key, value)
				prev = client.Get(key)
				if prev != next {
					t.Fatalf("wrong value; %s expected, but got %s", next, prev)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("  ... Passed\n")

	atomic.StoreInt32(&done, 1)
	checkConsistency(t, servers)

	wgTest.Wait()
}

func TestDoneCollector(t *testing.T) {
	//t.SkipNow()
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 3
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)

	var clientId int64 = 1

	const nclients = 5
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		// clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
		clients[i] = newClient([]string{addresses[0]}, atomic.AddInt64(&clientId, 1))
	}

	startServer(t, servers)

	fmt.Printf("Test: the done instances collector frees Paxos log memory...\n")

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	objectSize := 1000000 // 100K

	iters := 10

	value := make([]byte, objectSize)
	for j := 0; j < len(value); j++ {
		value[j] = byte((rand.Int() % 26) + 65)
	}

	for it := 0; it < iters; it++ {
		fmt.Printf("Iter %v begins\n", it)
		var wg sync.WaitGroup
		for i, _ := range clients {
			client := clients[i]
			key := "a" + strconv.Itoa(i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for k := 0; k < 20; k++ {
					value2 := make([]byte, objectSize)
					copy(value2, value)
					client.Put(key, string(value2))
				}
			}()
		}
		wg.Wait()

		time.Sleep(2 * time.Second)
	}
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	fmt.Printf("  Memory: before %v, after %v\n", m0.Alloc, m1.Alloc)

	if int(m1.Alloc-m0.Alloc) > objectSize*nservers*nclients*5 {
		t.Fatalf("memory does not shrink enough")
	}
	checkConsistency(t, servers)

	fmt.Printf("  ... Passed\n")
}

func TestTransaction(t *testing.T) {
	runtime.GOMAXPROCS(goMaxProcs)

	const nservers = 3
	servers, addresses := createServers(t, nservers)
	defer cleanup(servers)

	var clientId int64 = 1

	const nclients = 5
	clients := make([]*Client, nclients)
	for i, _ := range clients {
		// clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
		clients[i] = newClient(addresses, atomic.AddInt64(&clientId, 1))
	}

	startServer(t, servers)

	transactionId := int64(1)

	itercnt := 2000

	var wg sync.WaitGroup
	for i0, _ := range clients {
		client := clients[i0]
		wg.Add(1)
		i := i0
		go func() {
			defer wg.Done()
			key_i := 0
			for k := 0; k < itercnt; k++ {
				key_i++
				key := strconv.Itoa(i) + "-" + strconv.Itoa(key_i)
				op := client.CreateBundledOp(&Transaction{
					ClientId:      "table" + strconv.Itoa(i),
					TransactionId: atomic.AddInt64(&transactionId, 1),
					TableId:       "table" + strconv.Itoa(i),
					Ops: []*AnyOp{OpMapStore([]string{}, key, key).
						OpSetTableOption(CreateTableOption_UseTransactionTableId, false),
						OpMapStore([]string{}, "12", "1234323"),
						// we create a new map with key 23456 in the root map here
						// note: it will replace the old value.
						OpMapStore([]string{}, "23456", map[string]*AnyValue{}),
						// we then visit the nested map
						// by creating a new list in it.
						// note: it will replace the old value.
						OpMapStore([]string{"23456"}, "nested-list", []*AnyValue{}),
						// append to this list
						OpAppend([]string{"23456", "nested-list"}, "1Ws411Z7CJ"),
						OpAppend([]string{"23456", "nested-list"}, "1us411B7uN"),
						OpAppend([]string{"23456", "nested-list"}, []byte("1235o"))},
					TableVersionExpected: -1,
				})
				res := client.Submit(&BatchSubmitArgs{Wrapper: op})
				if !res.OK {
					t.Fatalf("not ok")
				}
			}
		}()
	}
	wg.Wait()

	for i, client := range clients {
		key_i := 0
		for k := 0; k < itercnt; k++ {
			key_i++
			key := strconv.Itoa(i) + "-" + strconv.Itoa(key_i)
			op := client.CreateBundledOp(&Transaction{
				ClientId:             "table" + strconv.Itoa(i),
				TransactionId:        atomic.AddInt64(&transactionId, 1),
				TableId:              "table" + strconv.Itoa(i),
				Ops:                  []*AnyOp{OpGet([]string{key}, "d")},
				TableVersionExpected: -1,
			})
			result := client.Submit(&BatchSubmitArgs{Wrapper: op})
			if !result.OK {
				t.Fatalf("unexpected not ok")
			}
			v := result.Result.TransactionResults[0].Values["d"].Value
			if v.Str != key {
				t.Fatalf("value not match")
			}
		}
	}
	checkConsistency(t, servers)
}
