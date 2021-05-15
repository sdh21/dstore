package paxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
	"strconv"
	"sync"
)

type PersistentStorage interface {
	Write(key string, size int64, value []byte) error
	Delete(key string) error
}

const logPrefix = "PAXOSLOG"
const logSuffix = "PAXOSLOGEND"

func (px *Paxos) persistentPrepare(globalNp int64, instanceIdSeen int64) {
	key := "Prepare-" + strconv.Itoa(int(globalNp))

	// we only need to store Np; InstanceId=-1 indicates this is a Prepare log
	length, content := px.encodeInstance(&Instance{
		HighestAcN: -1,
		InstanceId: -1,
	}, globalNp, instanceIdSeen)
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
