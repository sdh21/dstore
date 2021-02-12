package kvdb

import (
	"context"
	"fmt"
	"github.com/sdh21/dstore/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// This client is for testing purpose only, though DBAccessLayer might
// reuse some code here.

// See DBAccessLayer in accesslayer.go. It implements
// a kvdb client that supports batch requests.

type ServerInfo struct {
	client KeyValueDBClient
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

func NewClient(servers []string, clientId int64, tlsConfig *utils.MutualTLSConfig) *Client {
	result := &Client{}
	result.clientId = clientId
	result.servers = make(map[string]*ServerInfo)

	_, clientTLSConfig, err := utils.LoadMutualTLSConfig(tlsConfig)
	if err != nil {
		panic("cannot load TLS for db client.")
	}

	for _, server := range servers {
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(
			credentials.NewTLS(clientTLSConfig)))
		if err != nil {
			utils.Warning("Cannot dial %v, %v", server, err)
			conn.Close()
		} else {
			client := NewKeyValueDBClient(conn)
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

// Pack multiple transactions into an OpWrapper
// Every transaction should have a unique client id
// and an incrementing request id.
func (c *Client) CreateBundledOp(t ...*Transaction) *OpWrapper {
	wrapId := atomic.AddInt64(&c.wrapCount, 1)
	wrapper := &OpWrapper{
		Count:        int64(len(t)),
		ForwarderId:  c.clientId,
		WrapperId:    wrapId,
		Transactions: t,
	}
	return wrapper
}

// blocks until submitted
func (c *Client) Submit(args *BatchSubmitArgs) *BatchSubmitReply {
	// fast path if we know whom we should talk to
	if c.talkTo != "" {
		utils.Info("client talking to %v", c.talkTo)
		ctx, fc := context.WithTimeout(context.Background(), 10000*time.Millisecond)
		reply, err := c.servers[c.talkTo].client.BatchSubmit(ctx, args, grpc.MaxCallRecvMsgSize(MaxGRPCMsgSize), grpc.MaxCallSendMsgSize(MaxGRPCMsgSize))
		fc()
		if err == nil && reply.OK {
			utils.Info("client %v talking to %v OK!", c.clientId, c.talkTo)
			return reply
		} else {
			utils.Info("client %v talking to %v FAILED!", c.clientId, c.talkTo)
			// do not believe the leader hint
			c.talkTo = ""
		}
	}

	for {
		for addr, server := range c.servers {
			utils.Info("client %v iterating server %v", c.clientId, addr)
			ctx, fc := context.WithTimeout(context.Background(), 10000*time.Millisecond)
			reply, err := server.client.BatchSubmit(ctx, args)
			fc()
			if err == nil && reply.OK {
				utils.Info("client %v iterating %v OK!", c.clientId, addr)
				c.talkTo = addr
				return reply
			} else {
				if err != nil {
					utils.Error("error: %v, reply: %v", err, reply)
				}
			}
		}
		time.Sleep(1000 * time.Millisecond)
	}
}

//  -----------------------------
//  The code below is for testing only
//  -----------------------------

func (c *Client) CreateUnaryOp(op *AnyOp) *OpWrapper {
	wrapId := atomic.AddInt64(&c.wrapCount, 1)
	op.OpSetTableOption(CreateTableOption_UseTransactionTableId, false)
	wrapper := &OpWrapper{
		Count:       1,
		ForwarderId: c.clientId,
		WrapperId:   wrapId,
		Transactions: []*Transaction{{
			ClientId:             "client" + strconv.Itoa(int(c.clientId)),
			TransactionId:        wrapId,
			TableId:              "default",
			Ops:                  []*AnyOp{op},
			TableVersionExpected: -1,
		}},
	}
	return wrapper
}

func (c *Client) Get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := &BatchSubmitArgs{
		Wrapper: c.CreateUnaryOp(OpGet([]string{key}, "result")),
	}
	reply := c.Submit(args)
	if len(reply.Result.TransactionResults) != 1 {
		log.Fatalf("not 1")
	}
	value, found := reply.Result.TransactionResults[0].Values["result"]
	if !found {
		log.Fatalf("cannot find value")
	}
	if value.IsNull {
		return ""
	}
	return value.Value.Str
}

func (c *Client) Put(key string, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := &BatchSubmitArgs{
		Wrapper: c.CreateUnaryOp(
			OpMapStore([]string{}, key, value)),
	}

	reply := c.Submit(args)

	if !reply.OK {
		fmt.Printf("debug here")
	}
	if reply.Result.TransactionResults[0].Status != TransactionResult_OK {
		fmt.Printf("debug here")
	}
}

func (c *Client) PutHash(key string, value string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	args := &BatchSubmitArgs{
		Wrapper: c.CreateUnaryOp(&AnyOp{
			Type: "PutHash",
			Key:  key,
			Value: &AnyValue{Type: "Str",
				Str: value},
		}),
	}

	reply := c.Submit(args)

	oldValue, found := reply.Result.TransactionResults[0].Values[key]
	if !found {
		log.Fatalf("cannot find value")
	}
	if oldValue.IsNull {
		return ""
	}
	return oldValue.Value.Str
}
