package kvstore

// KV Store Component - Client

import (
	"context"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"runtime"
	"sync"
	"time"
)

type ServerInfo struct {
	client KVStoreRPCClient
	conn   *grpc.ClientConn
}

type Client struct {
	servers   map[string]*ServerInfo
	clientId  int64
	wrapCount int64
	// we maintain our talkTo
	// which is the last succeeded server
	// do not believe what servers tell us (some may be in a partition)
	talkTo string
	// no concurrent client
	mu sync.Mutex
}

func NewClient(servers []string, clientId int64, tlsConfig *cert.MutualTLSConfig) *Client {
	result := &Client{}
	result.clientId = clientId
	result.servers = make(map[string]*ServerInfo)

	_, clientTLSConfig, err := cert.LoadMutualTLSConfig(tlsConfig)
	if err != nil {
		panic("cannot load TLS for db client.")
	}

	for _, server := range servers {
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(
			credentials.NewTLS(clientTLSConfig)))
		if err != nil {
			utils.Warning("Cannot dial %v, %v", server, err)
		} else {
			client := NewKVStoreRPCClient(conn)
			result.servers[server] = &ServerInfo{client: client,
				conn: conn}
		}
	}
	runtime.SetFinalizer(result, (*Client).Close)
	return result
}

func (c *Client) Close() error {
	for _, server := range c.servers {
		server.conn.Close()
	}
	return nil
}

func (c *Client) SubmitTransactions(t ...*Transaction) ([]*TransactionResult, bool) {
	args := &BatchSubmitArgs{
		Count:        int64(len(t)),
		Transactions: t,
	}
	var reply *BatchSubmitReply
	var err error
	// fast path if we know whom we should talk to
	if c.talkTo != "" {
		utils.Info("client talking to %v", c.talkTo)
		ctx, fc := context.WithTimeout(context.Background(), 10000*time.Millisecond)
		reply, err = c.servers[c.talkTo].client.BatchSubmit(ctx, args, grpc.MaxCallRecvMsgSize(MaxGRPCMsgSize), grpc.MaxCallSendMsgSize(MaxGRPCMsgSize))
		fc()
		if err == nil && reply.OK {
			utils.Info("client %v talking to %v OK!", c.clientId, c.talkTo)
		} else {
			utils.Info("client %v talking to %v FAILED!", c.clientId, c.talkTo)
			// do not believe the leader hint
			c.talkTo = ""
		}
	}
	if reply == nil {
		for {
			for addr, server := range c.servers {
				utils.Info("client %v iterating server %v", c.clientId, addr)
				ctx, fc := context.WithTimeout(context.Background(), 10000*time.Millisecond)
				reply, err = server.client.BatchSubmit(ctx, args)
				fc()
				if err == nil && reply.OK {
					utils.Info("client %v iterating %v OK!", c.clientId, addr)
					c.talkTo = addr
					break
				} else {
					if err != nil {
						utils.Error("error: %v, reply: %v", err, reply)
					}
				}
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}

	return reply.TransactionResults, reply.OK
}
