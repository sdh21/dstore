package main

import (
	"fmt"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/example/FileServer"
	"github.com/sdh21/dstore/example/StorageServer"
	"github.com/sdh21/dstore/kvstore"
	"github.com/sdh21/dstore/paxos"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func usage() {
	fmt.Print("Usage: db/storage-server/file-server config-file id action.\n" +
		"DB Action: start, stop, recover\n" +
		"Storage Server Action: start, stop, recover, check-integrity\n" +
		"File Server Action: start, stop\n")
}

func testStartAll() {
	os.RemoveAll("/tmp/storage_server")
	os.RemoveAll("/tmp/dstore")

	dir, _ := os.Getwd()
	fmt.Println(dir)
	configFile := "./example/local_config.yaml"
	cfg, err := loadYamlConfig(configFile)
	if err != nil {
		log.Fatalf("cannot load %v\n", configFile)
	}

	for i, _ := range cfg.DBServers.Servers {
		go runDBServer(int64(i), cfg)
	}

	for i, _ := range cfg.StorageServers.Servers {
		go runStorageServer(int64(i), cfg)
	}

	for i, _ := range cfg.FileServers.Servers {
		go runFileServer(int64(i), cfg)
	}

	for {
		time.Sleep(10000000 * time.Second)
	}

}

func main() {
	testStartAll()
	return

	if len(os.Args) != 5 {
		usage()
		return
	}

	service := os.Args[1]
	configFile := os.Args[2]
	id, err := strconv.ParseInt(os.Args[3], 10, 32)
	if err != nil {
		usage()
		return
	}
	action := os.Args[3]

	cfg, err := loadYamlConfig(configFile)
	if err != nil {
		log.Fatalf("cannot load %v\n", configFile)
	}

	switch service {
	case "benchmark":
		fmt.Println("todo")
	case "db":
		if int(id) >= len(cfg.DBServers.Servers) {
			log.Fatalf("invalid id\n")
		}
		switch action {
		case "start":
			runDBServer(id, cfg)
		default:
			fmt.Println("todo")
		}
	case "storage-server":
		if int(id) >= len(cfg.StorageServers.Servers) {
			log.Fatalf("invalid id\n")
		}
		switch action {
		case "start":
			runStorageServer(id, cfg)
		default:
			fmt.Println("todo")
		}
	case "file-server":
		if int(id) >= len(cfg.FileServers.Servers) {
			log.Fatalf("invalid id\n")
		}
		switch action {
		case "start":
			runFileServer(id, cfg)
		default:
			fmt.Println("todo")
		}
	default:
		fmt.Println("unknown service")
		usage()
	}

}

func runFileServer(idx int64, cfg *yamlConfig) {
	this := cfg.FileServers.Servers[idx]

	var dbServers []string

	for _, s := range cfg.DBServers.Servers {
		dbServers = append(dbServers, s.DBAddr)
	}

	dbTLSConfig := &cert.MutualTLSConfig{
		ServerCertFile: cfg.DBServers.TLSServerCert,
		ServerPKFile:   cfg.DBServers.TLSServerKey,
		ClientCertFile: cfg.DBServers.TLSClientCert,
		ClientPKFile:   cfg.DBServers.TLSClientKey,
	}

	sharedStorage := FileServer.NewTemporaryStorage(idx, dbServers, dbTLSConfig, this.HttpBindOn)

	server := FileServer.New(sharedStorage)

	for _, s := range cfg.StorageServers.Servers {
		err := server.FileService.RegisterStorageProvider(s.Name, s.Addr, &cert.MutualTLSConfig{
			ServerCertFile: cfg.StorageServers.TLSServerCert,
			ServerPKFile:   cfg.StorageServers.TLSServerKey,
			ClientCertFile: cfg.StorageServers.TLSClientCert,
			ClientPKFile:   cfg.StorageServers.TLSClientKey,
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	server.Start()

	for {
		time.Sleep(100000 * time.Second)
	}
}

func runStorageServer(idx int64, cfg *yamlConfig) {
	this := cfg.StorageServers.Servers[idx]

	server, err := StorageServer.NewStorageServer(this.Folder, &cert.MutualTLSConfig{
		ServerCertFile: cfg.StorageServers.TLSServerCert,
		ServerPKFile:   cfg.StorageServers.TLSServerKey,
		ClientCertFile: cfg.StorageServers.TLSClientCert,
		ClientPKFile:   cfg.StorageServers.TLSClientKey,
	})

	if err != nil {
		log.Fatal(err)
	}

	l, e := net.Listen("tcp", this.BindOn)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.StartServer(l, this.HttpBindOn)

	for {
		time.Sleep(100000 * time.Second)
	}
}

func runDBServer(idx int64, cfg *yamlConfig) {
	this := cfg.DBServers.Servers[idx]

	dbConfig := kvstore.DefaultConfig()
	dbConfig.StorageFolder = this.DBFolder

	var paxosPeers []*paxos.Peer
	if len(cfg.DBServers.Servers) < 3 {
		log.Fatal("there should be at least 3 servers")
	}
	for _, peer := range cfg.DBServers.Servers {
		paxosPeers = append(paxosPeers, &paxos.Peer{
			ServerName:        peer.Name,
			Address:           peer.PaxosAddr,
			MinimumInstanceId: 0,
		})
	}

	l, e := net.Listen("tcp", this.PaxosBindOn)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	dbConfig.PaxosConfig.MaxDoneInstancesInMemory = 20000
	dbConfig.PaxosConfig.Listener = l
	dbConfig.PaxosConfig.Peers = paxosPeers
	dbConfig.PaxosConfig.Me = int(this.ID)
	dbConfig.PaxosFolder = this.PaxosFolder
	dbConfig.PaxosConfig.TLS = &cert.MutualTLSConfig{
		ServerCertFile: cfg.DBServers.PaxosTLS.TLSServerCert,
		ServerPKFile:   cfg.DBServers.PaxosTLS.TLSServerKey,
		ClientCertFile: cfg.DBServers.PaxosTLS.TLSClientCert,
		ClientPKFile:   cfg.DBServers.PaxosTLS.TLSClientKey,
	}
	dbConfig.TLS = &cert.MutualTLSConfig{
		ServerCertFile: cfg.DBServers.TLSServerCert,
		ServerPKFile:   cfg.DBServers.TLSServerKey,
		ClientCertFile: cfg.DBServers.TLSClientCert,
		ClientPKFile:   cfg.DBServers.TLSClientKey,
	}
	dbConfig.SaveProcessedResults = false

	dbl, e := net.Listen("tcp", this.DBBindOn)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	dbConfig.Listener = dbl
	dbConfig.DBAddress = dbl.Addr().String()

	db, err := kvstore.NewServer(dbConfig)
	if err != nil {
		log.Fatal("cannot run db server, error: ", err)
	}
	db.StartServer()

	fmt.Printf("Server started.\n")
	go func() {
		// http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	for {
		time.Sleep(100000 * time.Second)
	}
}

type yamlConfig struct {
	FileServers struct {
		Servers []struct {
			ID         int64  `yaml:"id"`
			UseHTTPS   bool   `yaml:"use_https"`
			Domain     string `yaml:"domain"`
			HttpBindOn string `yaml:"http_bind_on"`
			HttpAddr   string `yaml:"http_addr"`
		} `yaml:"servers"`
	} `yaml:"file_servers"`
	StorageServers struct {
		TLSClientCert string `yaml:"client_cert"`
		TLSClientKey  string `yaml:"client_key"`
		TLSServerCert string `yaml:"server_cert"`
		TLSServerKey  string `yaml:"server_key"`
		Servers       []struct {
			ID         int64  `yaml:"id"`
			Name       string `yaml:"name"`
			Domain     string `yaml:"domain"`
			Folder     string `yaml:"folder"`
			BindOn     string `yaml:"bind_on"`
			Addr       string `yaml:"addr"`
			HttpBindOn string `yaml:"http_bind_on"`
			HttpAddr   string `yaml:"http_addr"`
		} `yaml:"servers"`
	} `yaml:"storage_servers"`
	DBServers struct {
		TLSClientCert string `yaml:"client_cert"`
		TLSClientKey  string `yaml:"client_key"`
		TLSServerCert string `yaml:"server_cert"`
		TLSServerKey  string `yaml:"server_key"`
		PaxosTLS      struct {
			TLSClientCert string `yaml:"client_cert"`
			TLSClientKey  string `yaml:"client_key"`
			TLSServerCert string `yaml:"server_cert"`
			TLSServerKey  string `yaml:"server_key"`
		} `yaml:"paxos"`
		Servers []struct {
			ID          int64  `yaml:"id"`
			Name        string `yaml:"name"`
			PaxosAddr   string `yaml:"paxos_addr"`
			DBAddr      string `yaml:"db_addr"`
			PaxosBindOn string `yaml:"paxos_bind_on"`
			DBBindOn    string `yaml:"db_bind_on"`
			PaxosFolder string `yaml:"paxos_folder"`
			DBFolder    string `yaml:"db_folder"`
		}
	} `yaml:"db"`
}

func loadYamlConfig(file string) (*yamlConfig, error) {
	ya, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	cfg := &yamlConfig{}
	err = yaml.Unmarshal(ya, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
