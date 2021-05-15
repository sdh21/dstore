package kvdb

import (
	"github.com/sdh21/dstore/utils"
	"hash/fnv"
	"sync"
)

type DBState struct {
	Tables                map[string]*TableState
	mu                    sync.RWMutex
	LastAppliedProposalId int64
}

type TableState struct {
	Data              *AnyValue
	ProcessedRequests map[string]*TransactionResult
	TableVersion      int64
	mu                sync.Mutex

	dirty                               bool
	lastIncrementalCheckPointProposalId int64
}

func NewDBState() *DBState {
	return &DBState{
		Tables: make(map[string]*TableState),
	}
}

func newTable() *TableState {
	table := &TableState{
		Data: &AnyValue{
			Type:    "Map",
			Map:     map[string]*AnyValue{},
			Version: 0,
		},
		ProcessedRequests: map[string]*TransactionResult{},
		TableVersion:      0,
	}
	return table
}

func (db *KeyValueDB) applyOpWrapper(state *DBState, wrapper *OpWrapper, id int64) *WrapperResult {
	result := &WrapperResult{}
	transactionCount := wrapper.Count
	if transactionCount != int64(len(wrapper.Transactions)) {
		result.TransactionResults = append(result.TransactionResults, &TransactionResult{
			Status:  TransactionResult_Unknown,
			Message: "corrupted Transaction structure",
		})
		return result
	}
	result.TransactionResults = make([]*TransactionResult, transactionCount)

	state.mu.Lock()
	defer state.mu.Unlock()
	state.LastAppliedProposalId = id
	applyWaitGroup := &sync.WaitGroup{}
	for idx, transaction := range wrapper.Transactions {
		ops := transaction.Ops

		table, found := state.Tables[transaction.TableId]
		if !found {
			if len(ops) == 0 {
				result.TransactionResults[idx] = &TransactionResult{
					Status:  TransactionResult_TableNotExists,
					Message: "",
				}
				continue
			} else {
				table = newTable()
				state.Tables[transaction.TableId] = table
			}
		}

		if found && len(ops) > 0 && ops[0].TableOps != nil {
			if ops[0].TableOps.ErrIfExists {
				result.TransactionResults[idx] = &TransactionResult{
					Status:  TransactionResult_TableAlreadyExists,
					Message: "",
				}
				continue
			}
		}

		table.dirty = true

		idxC := idx
		transactionC := transaction
		applyWaitGroup.Add(1)
		go func() {
			result.TransactionResults[idxC] = db.applyOpTransaction(table, transactionC)
			applyWaitGroup.Done()
		}()
	}
	applyWaitGroup.Wait()
	return result
}

func (db *KeyValueDB) applyOpTransaction(state *TableState, transaction *Transaction) *TransactionResult {
	// TODO: add rollback support
	state.mu.Lock()
	defer state.mu.Unlock()

	result, found := state.ProcessedRequests[transaction.ClientId]
	if found {
		if result.TransactionId == transaction.TransactionId {
			return result
		} else if result.TransactionId > transaction.TransactionId {
			result = &TransactionResult{}
			result.Status = TransactionResult_TooLate
			return result
		}
	}

	result = &TransactionResult{}
	result.Values = map[string]*AliasValue{}
	result.Status = TransactionResult_OK

	if transaction.TableVersionExpected != -1 &&
		transaction.TableVersionExpected != state.TableVersion {
		result.Status = TransactionResult_NotExpectedTableVersion
		return result
	}

	for _, trans := range transaction.Ops {
		err := trans.Apply(state, result)
		if err != nil {
			utils.Error("apply: %v", err)
		}
		if result.Status != TransactionResult_OK {
			break
		}
	}
	state.TableVersion++

	if result.Status == TransactionResult_OK {
		result.Message = "OK"
		result.TransactionId = transaction.TransactionId
		result.TableVersion = state.TableVersion
		state.ProcessedRequests[transaction.ClientId] = result
	}

	return result
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
