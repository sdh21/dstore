package paxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
)

// ------------------------------------------------
//   Paxos Instances Reconstruction/Recover

//  Reconstruct Paxos: load instances from disk files and
//       create a Paxos consistent to the one before
//       crashing. ( though consistent, it is not necessary equal;
//	     we only need to guarantee before an RPC is replied, the log is
//       persistent. )
//

type ReconstructionResult struct {
	MinimumInstanceId int64 // the smallest instance we can recover
	LogEntriesCount   int64 // how many log entries we've found
}

//  Reconstruct Paxos if server crashed
//  Should be called before StartServer
//  Warning: caller must pass a new storage(a new folder) to NewPaxos, different from where logFiles lie in, and
//  ReconstructPaxos cannot check this.
//  Recovered logs will be written to the Config.Storage by calling the Write method.

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
