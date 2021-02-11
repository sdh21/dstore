package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
)

type perfConfig struct {
	serverId  int64
	clientId  int64
	peers     string
	dbport    string
	dbservers string

	paxosId int64

	concurrentclient int64

	// for clientTemplate2 DBAccessLayer
	concurrentqs int64

	storagef string

	valuesize int64
}

func perf() {
	cfg := new(perfConfig)

	set := flag.NewFlagSet("perf", flag.PanicOnError)

	set.Int64Var(&cfg.serverId, "server", -1, "")
	set.Int64Var(&cfg.clientId, "client", -1, "")
	set.StringVar(&cfg.peers, "peers", "", "")
	set.StringVar(&cfg.dbport, "dbport", "", "")
	set.StringVar(&cfg.dbservers, "dbservers", "", "")
	set.StringVar(&cfg.storagef, "storage", "", "")
	set.Int64Var(&cfg.concurrentclient, "c", -1, "")
	set.Int64Var(&cfg.concurrentqs, "q", -1, "")
	set.Int64Var(&cfg.valuesize, "valuesize", -1, "")

	set.Int64Var(&cfg.paxosId, "paxos", -1, "")

	set.Parse(os.Args[2:])
	debug.SetMaxThreads(30000)

	if cfg.paxosId != -1 {
		paxosPerf(cfg)
		return
	}

	if cfg.serverId != -1 {
		serverTemplate1(cfg)
	} else if cfg.clientId != -1 {
		if cfg.concurrentclient == -1 {
			panic("invalid concurrent client count")
		}
		if cfg.concurrentqs == -1 {
			clientTemplate1(cfg)
		} else {
			clientTemplate2(cfg)
		}

	} else {
		fmt.Printf("nothing is running.\n")
	}
}
