package main

import (
	"fmt"
	"github.com/sdh21/dstore/cert"
	"github.com/sdh21/dstore/kvstore"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client template 1 uses the raw kvdb client (no batch), and submits a complex transaction.
func clientTemplate1(cfg *perfConfig) {
	if cfg.valuesize == -1 {
		panic("invalid value size")
	}

	value := make([]byte, cfg.valuesize)
	rand.Read(value)

	// run a db client
	servers := strings.Split(cfg.dbservers, ",")

	fmt.Printf("Client started with template 1.\n")
	fmt.Printf("Client ID: %v\n", "client"+strconv.Itoa(int(cfg.clientId)))
	fmt.Printf("Key Size: %v KB\n", cfg.valuesize/1024)

	transactionCount := int64(0)

	mu := sync.Mutex{}

	go func() {
		for {
			t := time.Now()
			mu.Lock()
			atomic.StoreInt64(&transactionCount, 0)
			mu.Unlock()
			time.Sleep(10 * time.Second)
			mu.Lock()
			c := atomic.LoadInt64(&transactionCount)
			mu.Unlock()
			fmt.Printf("%v time passes, Transaction count: %v TPS(past 10 seconds): %v\n",
				time.Since(t), c, c/int64(time.Since(t).Seconds()))
		}
	}()

	// create concurrent sub-clients
	for subclienti := 0; subclienti < int(cfg.concurrentclient); subclienti++ {
		subclientId := (subclienti+1)<<16 | int(cfg.clientId)
		client := kvstore.NewClient(servers, int64(subclientId), cert.TestTlsConfig())
		go func() {
			requestId := int64(1)
			for {
				requestId++
				op := client.CreateBundledOp(&kvstore.Transaction{
					ClientId:      "template1" + strconv.Itoa(int(subclientId)),
					TransactionId: requestId,
					CollectionId:  "template1" + strconv.Itoa(int(subclientId)),
					Ops: []*kvstore.AnyOp{
						kvstore.OpMapStore([]string{}, "1", "1").
							OpSetTableOption(kvstore.CreateTableOption_UseTransactionTableId, false),
						kvstore.OpMapStore([]string{}, "12", "1234323"),
						// we create a new map with key 23456 in the root map here
						// note: it will replace the old value.
						kvstore.OpMapStore([]string{}, "23456", map[string]*kvstore.AnyValue{}),
						// we then visit the nested map
						// by creating a new list in it.
						// note: it will replace the old value.
						kvstore.OpMapStore([]string{"23456"}, "nested-list", []*kvstore.AnyValue{}),
						// append to this list
						kvstore.OpAppend([]string{"23456", "nested-list"}, "1111111111"),
						kvstore.OpAppend([]string{"23456", "nested-list"}, "1111111111"),
						kvstore.OpAppend([]string{"23456", "nested-list"}, value),
					},
					TableVersionExpected: -1,
				})
				reply := client.Submit(&kvstore.BatchSubmitArgs{
					Wrapper: op,
				})
				if !reply.OK {
					fmt.Printf("err: reply: %v\n", reply)
					time.Sleep(1 * time.Second)
				} else if len(reply.Result.TransactionResults) != 1 {
					fmt.Printf("len wrong\n")
				} else if reply.Result.TransactionResults[0].Status !=
					kvstore.TransactionResult_OK {
					fmt.Printf("err: reply: %v\n", reply)
				} else {
					mu.Lock()
					atomic.AddInt64(&transactionCount, 1)
					mu.Unlock()
				}

			}
		}()
	}

	for {
		time.Sleep(1000000 * time.Second)
	}

}

// Client template 2 uses the DBAccessLayer, and submits complex transactions.
func clientTemplate2(cfg *perfConfig) {
	if cfg.valuesize == -1 {
		panic("invalid value size")
	}

	value := make([]byte, cfg.valuesize)
	rand.Read(value)

	// run a db client
	servers := strings.Split(cfg.dbservers, ",")
	if cfg.concurrentqs == -1 {
		panic("incorrect q")
	}
	client := kvstore.NewDBAccessLayer(uint64(cfg.concurrentqs), servers, cfg.clientId, cert.TestTlsConfig())
	fmt.Printf("Client started with template 2.\n")
	fmt.Printf("Client ID: %v\n", "client"+strconv.Itoa(int(cfg.clientId)))
	fmt.Printf("Key Size: %v KB\n", cfg.valuesize/1024)

	transactionCount := int64(0)

	// legacy lock
	mu := sync.Mutex{}

	go func() {
		for {
			t := time.Now()
			mu.Lock()
			atomic.StoreInt64(&transactionCount, 0)
			mu.Unlock()
			time.Sleep(10 * time.Second)
			mu.Lock()
			c := atomic.LoadInt64(&transactionCount)
			mu.Unlock()
			fmt.Printf("%v time passes, Transaction count: %v TPS(past 10 seconds): %v\n",
				time.Since(t), c, c/int64(time.Since(t).Seconds()))
		}
	}()

	// create concurrent sub-clients
	for subclienti := 0; subclienti < int(cfg.concurrentclient); subclienti++ {
		subclientId := (subclienti+1)<<16 | int(cfg.clientId)
		go func() {
			requestId := int64(1)
			for {
				requestId++
				// db access layer
				reply := client.Submit(&kvstore.Transaction{
					ClientId:      "subclient" + strconv.Itoa(subclientId),
					TransactionId: requestId,
					CollectionId:  "subclient" + strconv.Itoa(subclientId),
					Ops: []*kvstore.AnyOp{
						kvstore.OpMapStore([]string{}, "1", "1").
							OpSetTableOption(kvstore.CreateTableOption_UseTransactionTableId, false),
						kvstore.OpMapStore([]string{}, "12", "1234323"),
						// we create a new map with key 23456 in the root map here
						// note: it will replace the old value.
						kvstore.OpMapStore([]string{}, "23456", map[string]*kvstore.AnyValue{}),
						// we then visit the nested map
						// by creating a new list in it.
						// note: it will replace the old value.
						kvstore.OpMapStore([]string{"23456"}, "nested-list", []*kvstore.AnyValue{}),
						// append to this list
						kvstore.OpAppend([]string{"23456", "nested-list"}, "1111111111"),
						kvstore.OpAppend([]string{"23456", "nested-list"}, "1111111111"),
						kvstore.OpAppend([]string{"23456", "nested-list"}, value),
					},
					TableVersionExpected: -1,
				})
				// reply from db access layer can be nil
				if reply == nil {
					fmt.Printf("db access layer fails\n")
					time.Sleep(1 * time.Second)
				} else if reply.Status != kvstore.TransactionResult_OK {
					fmt.Printf("err: reply: %v\n", reply)
				} else {
					mu.Lock()
					atomic.AddInt64(&transactionCount, 1)
					mu.Unlock()
				}
			}
		}()
	}

	for {
		time.Sleep(1000000 * time.Second)
	}
}
