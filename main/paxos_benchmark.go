package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/storage"
	"github.com/sdh21/dstore/utils"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
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

func paxosPerf(cfg *perfConfig) {

	fo := cfg.storagef + "/px-" + strconv.Itoa(int(cfg.paxosId))
	os.RemoveAll(fo)
	os.MkdirAll(fo, 0777)

	sg := storage.NewEmptyStorage(fo, 64*1024*1024, 4*1024)
	stats := sg.CreateBlockGroup(4*1024, "/log-")

	dsk := &DiskStorage{
		storage:      sg,    // sg
		storageStats: stats, //stats
		key2block:    map[string][]*storage.FileBlockInfo{},
		folder:       fo,
	}
	config := &paxos.ServerConfig{
		Peers:                        nil,
		Me:                           int(cfg.paxosId),
		Storage:                      dsk,
		Timeout:                      5000 * time.Millisecond,
		RandomBackoffMax:             3000,
		HeartbeatInterval:            300,
		LeaderTimeoutRTTs:            10,
		LeaderTimeout:                10000,
		DecideChannel:                nil,
		Listener:                     nil,
		AdditionalTimeoutPer100Holes: 100 * time.Millisecond,
	}

	var paxosPeers []*paxos.Peer
	p := strings.Split(cfg.peers, ",")
	for i, peer := range p {
		paxosPeers = append(paxosPeers, &paxos.Peer{
			ServerName:        strconv.Itoa(i),
			Address:           peer,
			MinimumInstanceId: 0,
		})
	}

	myPaxosAddress := paxosPeers[cfg.paxosId].Address
	myPaxosPort := strings.Split(myPaxosAddress, ":")[1]

	l, e := net.Listen("tcp", "0.0.0.0:"+myPaxosPort)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	config.Listener = l
	config.Peers = paxosPeers
	px, err := paxos.NewPaxos(config, utils.TestTlsConfig())
	if err != nil {
		panic(err)
	}
	type payload struct {
		Id int64
		S  []byte
	}
	gob.Register(&payload{})
	s := make([]byte, cfg.valuesize)
	id := int64(0)
	rand.Read(s)
	px.StartServer()
	fmt.Printf("Paxos %v started, listening on %v, concurrently proposing %v values.\n", config.Me, myPaxosPort,
		cfg.concurrentclient)
	cnt := int64(0)

	go func() {
		for {
			t := time.Now()
			atomic.StoreInt64(&cnt, 0)
			time.Sleep(10 * time.Second)
			c := atomic.LoadInt64(&cnt)
			fmt.Printf("%v time passes, count: %v consensus per second(past 10 seconds): %v\n",
				time.Since(t), c, c/int64(time.Since(t).Seconds()))
		}
	}()

	go func() {
		for {
			px.Done(px.MaxSequential())
			time.Sleep(1 * time.Second)
		}
	}()

	for th := 0; th < int(cfg.concurrentclient); th++ {
		go func() {
			for {
				pl := &payload{
					Id: atomic.AddInt64(&id, 1),
					S:  s,
				}
				res := px.ProposeValue(pl, func(i interface{}, i2 interface{}) bool {
					if i == nil || i2 == nil {
						return false
					}
					return i.(*payload).Id == i2.(*payload).Id
				})
				if !res.Succeeded {
					time.Sleep(10 * time.Second)
					if res.Reason == paxos.FailReasonProposerNotLeader {
						time.Sleep(10 * time.Second)
					} else {
						//println(res)
					}
				} else {
					px.Done(res.InstanceId)
					atomic.AddInt64(&cnt, 1)
				}
			}
		}()
	}

	for {
		time.Sleep(1000000 * time.Second)
	}

}
