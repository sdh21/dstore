package kvdb

import (
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/storage"
	"os"
)

func initializePaxos(config *DBConfig) (*paxos.Paxos, error) {
	folder := config.PaxosFolder
	sg, err := storage.NewStorage(folder, config.StorageBlockSize, config.AllocationUnit)
	if err != nil {
		return nil, err
	}
	err = os.Mkdir(folder+"/logs", os.FileMode(0777))
	if err != nil {
		return nil, err
	}
	stats := sg.CreateBlockGroup(config.AllocationUnit, "/logs/log-")

	dsk := &DiskStorage{
		storage:      sg,    // sg
		storageStats: stats, //stats
		key2block:    map[string][]*storage.FileBlockInfo{},
		folder:       folder,
	}
	config.PaxosConfig.Storage = dsk
	px, err := paxos.NewPaxos(config.PaxosConfig)
	if err != nil {
		return nil, err
	}
	return px, nil
}
