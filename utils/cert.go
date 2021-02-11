package utils

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
)

type MutualTLSConfig struct {
	ServerCertFile string
	ServerPKFile   string
	ClientCertFile string
	ClientPKFile   string
}

func TestTlsConfig() *MutualTLSConfig {
	return &MutualTLSConfig{
		ServerCertFile: "../cert/test_cert/server.crt",
		ServerPKFile:   "../cert/test_cert/server.key",
		ClientCertFile: "../cert/test_cert/client.crt",
		ClientPKFile:   "../cert/test_cert/client.key",
	}
}

// Load mutual TLS config. Returns ServerConfig, ClientConfig, error.
func LoadMutualTLSConfig(config *MutualTLSConfig) (*tls.Config, *tls.Config, error) {
	return loadTLSConfig(config.ServerCertFile, config.ServerPKFile,
		config.ClientCertFile, config.ClientPKFile)
}

// Load mutual TLS config. Returns ServerConfig, ClientConfig, error.
func loadTLSConfig(serverCertFile string, serverPKFile string,
	clientCertFile string, clientPKFile string) (*tls.Config, *tls.Config, error) {
	serverKeyPair, err := tls.LoadX509KeyPair(serverCertFile, serverPKFile)
	if err != nil {
		return nil, nil, err
	}
	clientKeyPair, err := tls.LoadX509KeyPair(clientCertFile, clientPKFile)
	if err != nil {
		return nil, nil, err
	}
	serverCert, err := ioutil.ReadFile(serverCertFile)
	if err != nil {
		return nil, nil, err
	}
	clientCert, err := ioutil.ReadFile(clientCertFile)
	if err != nil {
		return nil, nil, err
	}

	serverCertPool := x509.NewCertPool()
	ok := serverCertPool.AppendCertsFromPEM(clientCert)
	if !ok {
		return nil, nil, errors.New("cannot load client cert")
	}
	serverConfig := &tls.Config{
		Certificates:           []tls.Certificate{serverKeyPair},
		ClientAuth:             tls.RequireAndVerifyClientCert,
		ClientCAs:              serverCertPool,
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
		ClientSessionCache:     nil,
	}

	clientCertPool := x509.NewCertPool()
	ok = clientCertPool.AppendCertsFromPEM(serverCert)
	if !ok {
		return nil, nil, errors.New("cannot load server cert")
	}

	clientConfig := &tls.Config{
		Certificates:           []tls.Certificate{clientKeyPair},
		ClientAuth:             tls.RequireAndVerifyClientCert,
		RootCAs:                clientCertPool,
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
		ClientSessionCache:     nil,
	}

	return serverConfig, clientConfig, nil
}
