package kvdb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/sdh21/dstore/storage"
	"log"
	"sync/atomic"
	"time"
)

// No lock needed, as checkpoint is done one by one
type checkpointState struct {
	lastCheckpointVersion    map[string]int64
	lastCheckpointProposalId map[string]int64
	diskStorage              *storage.Storage
	// old checkpoint for a table. If a new one is synced to disk,
	// the old checkpoint can be deleted.
	lastCheckpointKey map[string]string
}

// A simple checkpoint, which routinely scans dirty tables
// and writes them to disk.
// The minimum work unit is a table for simplicity.
// After a checkpoint is generated, we can tell Paxos to free
// the log instances.
func (db *KeyValueDB) startCheckpointRoutine() {
	go func() {
		lastProposalId := int64(-1)
		dirtyTables := make(map[string]*TableState)

		for {

			time.Sleep(100 * time.Millisecond)

			if atomic.LoadInt32(&db.closed) == 1 {
				break
			}

			if lastProposalId == -1 {
				db.state.mu.RLock()
				if len(db.state.tables) == 0 {
					db.state.mu.RUnlock()
					continue
				}
				lastProposalId = db.state.lastProposalId
				dirtyTables = make(map[string]*TableState)
				// find all dirty tables
				for id, table := range db.state.tables {
					if db.checkpoint.lastCheckpointProposalId[id] > lastProposalId {
						log.Fatalf("cannot happen")
					}
					if db.checkpoint.lastCheckpointProposalId[id] == lastProposalId {
						continue
					}
					db.checkpoint.lastCheckpointProposalId[id] = lastProposalId
					if db.checkpoint.lastCheckpointVersion[id] == table.TableVersion {
						continue
					}
					db.checkpoint.lastCheckpointVersion[id] = table.TableVersion
					dirtyTables[id] = table
				}
				db.state.mu.RUnlock()
			}

			if len(dirtyTables) == 0 {
				db.px.Done(lastProposalId)
				// logger.Warning("Checkpoint %v finished, asking Paxos to free instances <= %v", db.px.GetMe(), lastProposalId)
				lastProposalId = -1
				continue
			}

			// dump dirty tables

			for id, table := range dirtyTables {
				buf := &bytes.Buffer{}
				enc := gob.NewEncoder(buf)

				table.mu.Lock()
				version := table.TableVersion
				err := enc.Encode(table)
				if err != nil {
					log.Fatalf("cannot encode table: %v", err)
				}
				table.mu.Unlock()

				snapshotKey := fmt.Sprintf("%v/%x/%x", id, version, lastProposalId)
				_, err = db.checkpoint.diskStorage.CreateSmallFile(snapshotKey, int64(len(buf.Bytes())), buf.Bytes())
				if err != nil {
					log.Fatalf("cannot create storage file: %v", err)
				}
				// Now we've synced the new checkpoint to the disk
				lastKey, lastKeyFound := db.checkpoint.lastCheckpointKey[id]
				if lastKeyFound {
					err = db.checkpoint.diskStorage.DeleteFile(lastKey)
					if err != nil {
						log.Fatalf("cannot delete storage file: %v %v", lastKey, err)
					}
				}
				db.checkpoint.lastCheckpointKey[id] = snapshotKey
			}

			// Now we've synced all checkpoint files to disk
			db.px.Done(lastProposalId)
			//logger.Warning("Checkpoint %v finished, asking Paxos to free instances <= %v", db.px.GetMe(), lastProposalId)
			lastProposalId = -1
		}
	}()
}

func (db *KeyValueDB) initializeCheckPoint(config *DBConfig) error {
	db.checkpoint = &checkpointState{}
	db.checkpoint.lastCheckpointVersion = map[string]int64{}
	db.checkpoint.lastCheckpointProposalId = map[string]int64{}
	db.checkpoint.lastCheckpointKey = map[string]string{}

	folder := config.StorageFolder + "/checkpoint"
	db.checkpoint.diskStorage = storage.NewEmptyStorage(folder, config.StorageBlockSize, config.AllocationUnit)

	return nil
}
