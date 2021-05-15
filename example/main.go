package example

import (
	"fmt"
	"github.com/sdh21/dstore/example/StorageServer"
	"github.com/sdh21/dstore/kvdb"
	"github.com/sdh21/dstore/paxos"
	"github.com/sdh21/dstore/utils"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: run benchmark/db/storage/forwarder/recover")
	}

	switch os.Args[1] {
	case "benchmark":

	case "db":

	}
}

func runAuthServer(idx int, cfg *yamlConfig) {

}

func runStorageServer(idx int, cfg *yamlConfig) {
	this := cfg.StorageServers.Servers[idx]

	server, err := StorageServer.NewStorageServer(this.Folder, &utils.MutualTLSConfig{
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

func runDBServer(idx int, cfg *yamlConfig) {
	this := cfg.DBServers.Servers[idx]

	dbConfig := kvdb.DefaultConfig()
	dbConfig.StorageFolder = this.DBFolder

	var paxosPeers []*paxos.Peer
	if len(cfg.DBServers.Servers) < 3 {
		log.Fatal("there should be at least 3 servers")
	}
	for _, peer := range cfg.DBServers.Servers {
		paxosPeers = append(paxosPeers, &paxos.Peer{
			ServerName:        peer.ServerName,
			Address:           peer.PaxosAddr,
			MinimumInstanceId: 0,
		})
	}

	l, e := net.Listen("tcp", this.PaxosBindOn)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	dbConfig.PaxosConfig.Listener = l
	dbConfig.PaxosConfig.Peers = paxosPeers
	dbConfig.PaxosConfig.Me = int(this.ID)
	dbConfig.PaxosFolder = this.PaxosFolder
	dbConfig.PaxosConfig.TLS = &utils.MutualTLSConfig{
		ServerCertFile: cfg.DBServers.PaxosTLS.TLSServerCert,
		ServerPKFile:   cfg.DBServers.PaxosTLS.TLSServerKey,
		ClientCertFile: cfg.DBServers.PaxosTLS.TLSClientCert,
		ClientPKFile:   cfg.DBServers.PaxosTLS.TLSClientKey,
	}
	dbConfig.TLS = &utils.MutualTLSConfig{
		ServerCertFile: cfg.DBServers.TLSServerCert,
		ServerPKFile:   cfg.DBServers.TLSServerKey,
		ClientCertFile: cfg.DBServers.TLSClientCert,
		ClientPKFile:   cfg.DBServers.TLSClientKey,
	}

	dbl, e := net.Listen("tcp", this.DBBindOn)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	dbConfig.Listener = dbl
	dbConfig.DBAddress = dbl.Addr().String()

	db, err := kvdb.NewServer(dbConfig)
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
	AuthServers struct {
		Servers []struct {
			ID      int64  `yaml:"id"`
			Cert    string `yaml:"cert"`
			CertKey string `yaml:"cert_key"`
		} `yaml:"servers"`
	} `yaml:"auth_servers"`
	StorageServers struct {
		TLSClientCert string `yaml:"client_cert"`
		TLSClientKey  string `yaml:"client_key"`
		TLSServerCert string `yaml:"server_cert"`
		TLSServerKey  string `yaml:"server_key"`
		Servers       []struct {
			ID         int64  `yaml:"id"`
			Cert       string `yaml:"cert"`
			CertKey    string `yaml:"cert_key"`
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
		}
		Servers []struct {
			ID          int64  `yaml:"id"`
			ServerName  string `yaml:"server_name"`
			PaxosAddr   string `yaml:"paxos_addr"`
			DBAddr      string `yaml:"db_addr"`
			PaxosBindOn string `yaml:"paxos_bind_on"`
			DBBindOn    string `yaml:"db_bind_on"`
			PaxosFolder string `yaml:"paxos_folder"`
			DBFolder    string `yaml:"db_folder"`
		}
	}
}

func loadYamlConfig() (*yamlConfig, error) {
	ya, err := ioutil.ReadFile("config.yaml")
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
