package paxos

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/sdh21/dstore/storage"
	"github.com/sdh21/dstore/utils"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// I am still working on paxos persistence.
// Test here is subject to change.

type DiskStorage struct {
	storage      *storage.Storage
	storageStats *storage.BlockStatistics
	mapLock      sync.Mutex
	key2block    map[string][]*storage.FileBlockInfo
	folder       string
	entries      map[string][]byte
}

func (dsk *DiskStorage) Write(key string, size int64, value []byte) error {
	blocks, err := dsk.storage.CreateLogFile(size, value, dsk.storageStats)
	if err != nil {
		return err
	}
	dsk.mapLock.Lock()
	dsk.key2block[key] = blocks
	dsk.entries[key] = value
	dsk.mapLock.Unlock()
	return nil
}

func (dsk *DiskStorage) Delete(key string) error {
	dsk.mapLock.Lock()
	blocks := dsk.key2block[key]
	delete(dsk.key2block, key)
	delete(dsk.entries, key)
	dsk.mapLock.Unlock()
	if blocks == nil {
		return errors.New("trying to release a non-existing file")
	}
	err := dsk.storage.DeleteLogFile(blocks, dsk.storageStats)
	return err
}

func getTestStorageConfig() *ServerConfig {
	return &ServerConfig{
		Peers:             nil,
		Me:                -1,
		Storage:           nil,
		Timeout:           10 * time.Second,
		RandomBackoffMax:  200,
		HeartbeatInterval: 1000,
		LeaderTimeout:     10000,
		LeaderTimeoutRTTs: 10,
	}
}

func TestEncodeDecodeInstance(t *testing.T) {
	px := &Paxos{}
	inst := &Instance{
		HighestAcN: 0,
		HighestAcV: px.encodeValue(nil),
	}
	length, encoded := px.encodeInstance(inst, 1, 2)
	if len(encoded) != int(length) {
		t.Fatalf("wrong len")
	}
	inst2, v1, v2, _ := px.decodeInstance(encoded)
	if !reflect.DeepEqual(inst, inst2) || v1 != 1 || v2 != 2 {
		t.Fatalf("not equal")
	}
	if px.decodeValue(inst.HighestAcV) != nil ||
		px.decodeValue(inst2.HighestAcV) != nil {
		t.Fatalf("not nil")
	}

	inst = &Instance{
		HighestAcN: -1,
		HighestAcV: px.encodeValue(nil),
	}
	length, encoded = px.encodeInstance(inst, 100, 2)
	if len(encoded) != int(length) {
		t.Fatalf("wrong len")
	}
	inst2, v1, v2, _ = px.decodeInstance(encoded)
	if !reflect.DeepEqual(inst, inst2) || v1 != 100 || v2 != 2 {
		t.Fatalf("not equal")
	}
	if px.decodeValue(inst.HighestAcV) != nil ||
		px.decodeValue(inst2.HighestAcV) != nil {
		t.Fatalf("not nil")
	}

	inst = &Instance{
		HighestAcN: 85,
		HighestAcV: px.encodeValue(4213),
	}
	length, encoded = px.encodeInstance(inst, 100, 2)
	if len(encoded) != int(length) {
		t.Fatalf("wrong len")
	}
	inst2, v1, v2, _ = px.decodeInstance(encoded)
	if !reflect.DeepEqual(inst, inst2) || v1 != 100 || v2 != 2 {
		t.Fatalf("not equal")
	}
	if px.decodeValue(inst.HighestAcV) != 4213 ||
		px.decodeValue(inst2.HighestAcV) != 4213 {
		t.Fatalf("not nil")
	}

	inst = &Instance{
		HighestAcN: 12385,
		HighestAcV: px.encodeValue("vv450c2"),
	}
	length, encoded = px.encodeInstance(inst, 100, 2)
	if len(encoded) != int(length) {
		t.Fatalf("wrong len")
	}
	inst2, v1, v2, _ = px.decodeInstance(encoded)
	if !reflect.DeepEqual(inst, inst2) || v1 != 100 || v2 != 2 {
		t.Fatalf("not equal")
	}
	if px.decodeValue(inst.HighestAcV) != "vv450c2" ||
		px.decodeValue(inst2.HighestAcV) != "vv450c2" {
		t.Fatalf("not nil")
	}

	for _, blen := range []int{539, 1023, 1024, 1025, 2014, 2048, 3129, 19392, 1959902} {
		buf := make([]byte, blen)
		rand.Read(buf)
		inst = &Instance{
			HighestAcN: 123852 + int64(blen),
			HighestAcV: buf,
		}
		length, encoded = px.encodeInstance(inst, 100, 2)
		if len(encoded) != int(length) {
			t.Fatalf("wrong len")
		}
		inst2, v1, v2, _ = px.decodeInstance(encoded)
		if !reflect.DeepEqual(inst, inst2) || v1 != 100 || v2 != 2 {
			t.Fatalf("not equal")
		}
	}
}

func getLogFiles(folder string) ([]string, error) {
	files, _ := ioutil.ReadDir(folder)
	var result []string
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "log-") {
			result = append(result, folder+"/"+file.Name())
		}
	}
	return result, nil
}

func dumpPaxos(px *Paxos) {
	fmt.Printf("Paxos-%v\n", px.me)
	fmt.Printf("highestInstanceAccepted: %v\n", px.highestInstanceAccepted)
	fmt.Printf("highestNpSeen: %v\n", px.highestNpSeen)
	fmt.Printf("Instances: \n")
	for i := px.instances.GetFirstIndex(); i <= px.instances.GetHighestIndex(); i++ {
		ins, ok := px.instances.GetAt(i)
		if !ok {
			fmt.Printf("instance not found??\n")
		}
		fmt.Printf("Instance %v: \n", ins.InstanceId)
		fmt.Printf("     Na: %v \n", ins.HighestAcN)
		fmt.Printf("     Nv: %v \n", ins.HighestAcV)
		fmt.Printf("     Decide: %v \n", ins.DecidedV)
	}
}

func isRecoveredPaxosOK(original *Paxos, recovered *Paxos, t *testing.T) {
	dump := func() {
		fmt.Printf("Original Paxos: -------\n")
		dumpPaxos(original)
		fmt.Printf("\nRecovered Paxos: -------\n")
		dumpPaxos(recovered)
	}
	if original.highestNpSeen != recovered.highestNpSeen {
		dump()
		if original.highestNpSeen > recovered.highestNpSeen {
			fmt.Printf("Reconstruct Paxos: highestNpSeen not equal, but might be a hint not written to disk")
		} else {
			t.Fatalf("Reconstruct Paxos: original.highestNpSeen < recovered.highestNpSeen")
		}
	}
	// not necessarily true, but should be true in test as we close paxos gracefully
	if original.highestInstanceAccepted < recovered.highestInstanceAccepted {
		dump()
		t.Fatalf("Reconstruct Paxos: highestInstanceAccepted not equal")
	}
	for i := original.instances.GetFirstIndex(); i <= original.instances.GetHighestIndex(); i++ {
		inst, ok := original.instances.GetAt(i)
		if !ok {
			dump()
			t.Fatalf("Reconstruct Paxos: original.instances.GetAt(i), i=%v", i)
		}
		inst2, ok2 := recovered.instances.GetAt(i)
		if !ok2 {
			dump()
			t.Fatalf("Reconstruct Paxos: recovered.instances.GetAt(i), i=%v", i)
		}
		if inst.InstanceId != inst2.InstanceId || inst.InstanceId != i {
			dump()
			t.Fatalf("Reconstruct Paxos: inst.InstanceId != inst2.InstanceId, i=%v", i)
		}
		if inst.HighestAcN != inst2.HighestAcN {
			dump()
			t.Fatalf("Reconstruct Paxos: inst.HighestAcN != inst2.HighestAcN, i=%v", i)
		}
		if !reflect.DeepEqual(inst.HighestAcV, inst2.HighestAcV) {
			dump()
			t.Fatalf("Reconstruct Paxos: inst.HighestAcV != inst2.HighestAcV, i=%v", i)
		}
	}
	// TestManyForget
	if original.instances.GetFirstIndex() < recovered.instances.GetFirstIndex() {
		dump()
		t.Fatalf("Reconstruct Paxos: GetFirstIndex ?")
	}
}

func pxRecover(pxa []*Paxos, t *testing.T) []*Paxos {
	for i := 0; i < len(pxa); i++ {
		pxa[i].Close()
	}
	for i := 0; i < len(pxa); i++ {
		for j := pxa[i].instances.GetFirstIndex(); j <= pxa[i].instances.GetHighestIndex(); j++ {
			s, ok := pxa[i].instances.GetAt(j)
			if !ok {
				t.Fatalf("????????")
			}
			s.lock.Lock()
		}
	}

	// check what have written
	for i := 0; i < len(pxa); i++ {
		px := pxa[i]
		stg := px.storage.(*DiskStorage)
		if len(stg.key2block) != len(stg.entries) {
			t.Fatalf("len(stg.key2block) != len(stg.entries)")
		}
		for k, b := range stg.key2block {
			if len(b) != 1 {
				t.Fatalf("should have only one block")
			}
			block := b[0]
			file, _ := os.Open(stg.folder + "/log-" + strconv.Itoa(int(block.Index)))
			file.Seek(int64(block.Offset*1024), 0)
			buf := make([]byte, block.Size)
			io.ReadFull(file, buf)
			file.Close()
			if !bytes.Equal(buf, stg.entries[k]) {
				t.Fatalf("written data wrong, check storage")
			}
		}
	}

	os.Mkdir("/var/tmp/paxos-test-recovered", 0777)
	pxRecoverd := make([]*Paxos, len(pxa))
	for i := 0; i < len(pxa); i++ {
		folder := "/var/tmp/paxos-test-recovered/server-" + strconv.Itoa(i)
		os.Mkdir(folder, 0777)
		for fi := 0; fi < 300; fi++ {
			os.Remove(folder + "/log-" + strconv.Itoa(fi))
		}
		sg := storage.NewEmptyStorage(folder, 64*1024*1024, 1*1024)
		stats := sg.CreateBlockGroup(1*1024, "/log-")
		dsg := &DiskStorage{
			storage:      sg,
			storageStats: stats,
			key2block:    map[string][]*storage.FileBlockInfo{},
			entries:      map[string][]byte{},
			folder:       folder,
		}
		cfg := getTestStorageConfig()
		cfg.Peers = pxa[i].peers
		cfg.Me = i
		cfg.Storage = dsg
		tlscfg := &utils.MutualTLSConfig{
			ServerCertFile: "../cert/test_cert/server.crt",
			ServerPKFile:   "../cert/test_cert/server.key",
			ClientCertFile: "../cert/test_cert/client.crt",
			ClientPKFile:   "../cert/test_cert/client.key",
		}
		px, err := NewPaxos(cfg, tlscfg)
		if err != nil {
			t.Fatalf("%v", err)
		}
		logFiles, err := getLogFiles("/var/tmp/paxos-test/server-" + strconv.Itoa(i))
		if err != nil {
			t.Fatalf("getLogFiles: %v", err)
		}
		result, err := px.ReconstructPaxos(logFiles)
		if err != nil {
			t.Fatalf("ReconstructPaxos: %v", err)
		}
		if result.LogEntriesCount != int64(len(pxa[i].storage.(*DiskStorage).key2block)) {
			t.Fatalf("ReconstructPaxos: too few log entries found")
		}
		isRecoveredPaxosOK(pxa[i], px, t)

		pxRecoverd = append(pxRecoverd, px)
	}
	return pxRecoverd
}

func createPaxosWithStorage(count int, tag string, t *testing.T) ([]*Peer, []*Paxos) {
	peers, listeners := createPeers(count, tag)
	pxs := make([]*Paxos, count)
	os.Mkdir("/var/tmp/paxos-test", 0777)
	for i := 0; i < count; i++ {
		folder := "/var/tmp/paxos-test/server-" + strconv.Itoa(i)
		os.Mkdir(folder, 0777)
		for fi := 0; fi < 300; fi++ {
			os.Remove(folder + "/log-" + strconv.Itoa(fi))
		}
		sg := storage.NewEmptyStorage(folder, 64*1024*1024, 1*1024)
		stats := sg.CreateBlockGroup(1*1024, "/log-")
		var err error
		cfg := getTestStorageConfig()
		cfg.Peers = peers
		cfg.Me = i
		cfg.Storage = &DiskStorage{
			storage:      sg,
			storageStats: stats,
			key2block:    map[string][]*storage.FileBlockInfo{},
			entries:      map[string][]byte{},
			folder:       folder,
		}
		tlscfg := &utils.MutualTLSConfig{
			ServerCertFile: "../cert/test_cert/server.crt",
			ServerPKFile:   "../cert/test_cert/server.key",
			ClientCertFile: "../cert/test_cert/client.crt",
			ClientPKFile:   "../cert/test_cert/client.key",
		}
		pxs[i], err = NewPaxos(cfg, tlscfg)
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
