package main

import (
	"fmt"
	"github.com/sdh21/dstore/kvdb"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/utils"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)
import _ "net/http/pprof"

func serverTemplate1(cfg *perfConfig) {
	// runs a server
	fo := cfg.storagef + "/db-" + strconv.Itoa(int(cfg.serverId))
	os.RemoveAll(fo + "/paxos")
	os.MkdirAll(fo+"/paxos", 0777)
	os.RemoveAll(fo + "/checkpoint")
	os.MkdirAll(fo+"/checkpoint/data-blocks", 0777)
	os.MkdirAll(fo+"/checkpoint/file-metadata-blocks", 0777)

	config := kvdb.DefaultConfig()
	config.StorageFolder = fo

	var paxosPeers []*paxos.Peer
	p := strings.Split(cfg.peers, ",")
	for i, peer := range p {
		paxosPeers = append(paxosPeers, &paxos.Peer{
			ServerName:        strconv.Itoa(i),
			Address:           peer,
			MinimumInstanceId: 0,
		})
	}

	myPaxosAddress := paxosPeers[cfg.serverId].Address
	myPaxosPort := strings.Split(myPaxosAddress, ":")[1]

	l, e := net.Listen("tcp", "0.0.0.0:"+myPaxosPort)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	config.PaxosConfig.Listener = l
	config.PaxosConfig.Peers = paxosPeers
	config.PaxosConfig.Me = int(cfg.serverId)

	dbl, e := net.Listen("tcp", "0.0.0.0:"+cfg.dbport)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	config.Listener = dbl
	config.DBAddress = dbl.Addr().String()

	db, err := kvdb.NewServer(config, utils.TestTlsConfig())
	if err != nil {
		log.Fatal("cannot run db server, error: ", err)
	}
	db.StartServer()

	fmt.Printf("Server started.\n")
	go func() {
		http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	for {
		time.Sleep(100000 * time.Second)
	}
}
