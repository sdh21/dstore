package kvdb

import (
	"bytes"
	"encoding/gob"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/storage"
	"log"
	"strconv"
)

type CheckpointConfig struct {
	FullCheckpointEveryNLog        int64
	FullCheckpointAfterNSeconds    int64
	IncrementalCheckpointEveryNLog int64
}

type checkpointInst struct {
	config CheckpointConfig

	eventChannel                 chan paxos.DecideEvent
	dbState                      *DBState
	lastFullCheckpointProposalId int64

	diskStorage *storage.Storage
	// old checkpoint for a table. If a new one is synced to disk,
	// the old checkpoint can be deleted.

	lastCheckpointKey map[string]string
}

func (db *KeyValueDB) initializeCheckPoint(config *DBConfig) error {
	db.checkpoint.config = config.Checkpoint
	db.checkpoint = &checkpointInst{}
	db.checkpoint.lastFullCheckpointProposalId = -1
	db.checkpoint.eventChannel = make(chan paxos.DecideEvent, 100)

	folder := config.StorageFolder + "/checkpoint"
	var err error
	db.checkpoint.diskStorage, err = storage.NewStorage(folder, config.StorageBlockSize, config.AllocationUnit)

	return err
}

// FIXME: temporary solution
// cp id = paxos id
// each table has a corresponding cp id indicating the increm or full?
func (cp *checkpointInst) incrementalCheckpoint() error {
	for name, table := range cp.dbState.Tables {
		if !table.dirty {
			continue
		}
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err := encoder.Encode(table.Data)
		if err != nil {
			return err
		}
		_, err = cp.diskStorage.CreateSmallFile("IncCP-"+
			strconv.FormatInt(cp.dbState.LastAppliedProposalId, 36)+
			"-"+name,
			int64(buf.Len()), buf.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// FIXME: temporary solution
func (cp *checkpointInst) fullCheckpoint() error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(cp.dbState)
	if err != nil {
		return err
	}
	_, err = cp.diskStorage.CreateSmallFile("FullCP-"+strconv.FormatInt(cp.dbState.LastAppliedProposalId, 36),
		int64(buf.Len()), buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// A simple checkpoint, which routinely scans dirty tables
// and writes them to disk.
// The minimum work unit is a table for simplicity.
// After a checkpoint is generated, we can tell Paxos to free
// the log instances.
func (db *KeyValueDB) startCheckpointRoutine() {
	checkpoint := db.checkpoint
	go func() {
		for {
			event, ok := <-db.checkpoint.eventChannel
			if !ok {
				break
			}
			_ = db.applyOpWrapper(checkpoint.dbState, event.DecidedValue.(*OpWrapper), event.InstanceId)
			if checkpoint.dbState.LastAppliedProposalId != event.InstanceId {
				log.Fatalf("??? checkpoint.go startCheckpointRoutine")
			}
			if event.InstanceId%checkpoint.config.IncrementalCheckpointEveryNLog == 0 {
				err := checkpoint.incrementalCheckpoint()
				if err != nil {
					log.Fatalf("cannot generate checkpoint")
				}
				db.px.Done(event.InstanceId)
			}

			if event.InstanceId%checkpoint.config.FullCheckpointEveryNLog == 0 {
				err := checkpoint.fullCheckpoint()
				if err != nil {
					log.Fatalf("cannot generate checkpoint")
				}
				db.px.Done(event.InstanceId)
			}
		}
	}()
}
